"""
Location-scoped read visibility (``DAGSTER_AUTH_LOCATION_VISIBILITY=granted``).

Hides code locations a user holds no role in. Applied per request, on top of
the location-scoped permissions in ``location_rbac.py``, and only for users
without a global group (``AuthUser.global_access``).

Two layers:

1. **Workspace** — the request context gets a ``CurrentWorkspace`` containing
   only the visible locations. Everything Dagster derives from the workspace
   disappears: locations, jobs, schedules, sensors, asset definitions,
   lineage, the launchpad.

2. **Instance** — runs, logs, backfills, instigator state and asset history
   live in instance storage, not the workspace. The request context gets a
   per-request copy of the ``DagsterInstance`` whose read methods hide
   records from other locations:

   - run lists/counts: filtered in the storage query, by the
     ``.dagster/repository`` tag (``repo@location``), so pagination holds
   - runs by id, run logs, captured stdout/stderr: hidden if the run belongs
     to a hidden location
   - backfills, schedule/sensor state lists: filtered after loading
   - asset keys and asset events: limited to assets defined in visible locations

This is hiding, not a security boundary: the GraphQL API was not designed for
per-user reads, and paths not listed above (e.g. raw run tag values, run
storage ids) are not filtered. Writes are enforced separately by
``location_rbac.py``.
"""

import logging
import threading
import time
import weakref
from collections import OrderedDict
from typing import Any, Dict, FrozenSet, Optional, Set, Type

logger = logging.getLogger(__name__)

# Never a real repository label: matches no runs when nothing is visible.
_NO_VISIBLE_LOCATION = "__authkit_no_visible_location__"

# How long repository labels read from run storage are reused.
_LABEL_CACHE_TTL = 30.0


# DagsterInstance read methods the per-request instance overrides. Checked at
# startup: if Dagster renames one, the override would silently stop applying.
FILTERED_INSTANCE_METHODS = (
    "get_run_records",
    "get_runs",
    "get_run_ids",
    "get_runs_count",
    "get_run_tags",
    "get_run_by_id",
    "get_run_record_by_id",
    "get_run_group",
    "all_logs",
    "logs_after",
    "get_records_for_run",
    "watch_event_logs",
    "compute_log_manager",
    "get_backfills",
    "get_backfill",
    "all_instigator_state",
    "get_asset_keys",
    "fetch_materializations",
    "fetch_observations",
    "fetch_failed_materializations",
    "get_latest_planned_materialization_info",
)


class RunNotVisibleError(Exception):
    """Raised when a hidden run's captured logs are requested."""


# ---------------------------------------------------------------------------
# Workspace
# ---------------------------------------------------------------------------

# Most recent process-wide workspaces (one per reload), each mapping a visible
# set to its filtered copy. Strong references: CurrentWorkspace is a frozen
# record in some Dagster versions and can't be weakly referenced. Bounded, so
# workspaces replaced by reloads are dropped.
_workspace_cache: "OrderedDict[int, tuple]" = OrderedDict()
_WORKSPACE_CACHE_SIZE = 4
_workspace_cache_lock = threading.Lock()


def filter_workspace(workspace: Any, visible_locations: FrozenSet[str]) -> Any:
    """
    Return a ``CurrentWorkspace`` with only the visible locations.

    Cached per (workspace, visible set): the process context replaces its
    workspace object on reload, which starts a new cache entry, and the
    expensive asset graph is built once per visible set rather than per request.

    Args:
        workspace:         The process-wide ``CurrentWorkspace``.
        visible_locations: Lowercased names of the locations to keep.
    """
    with _workspace_cache_lock:
        cached = _workspace_cache.get(id(workspace))
        if cached is None or cached[0] is not workspace:
            cached = (workspace, {})
            _workspace_cache[id(workspace)] = cached
            while len(_workspace_cache) > _WORKSPACE_CACHE_SIZE:
                _workspace_cache.popitem(last=False)
        _workspace_cache.move_to_end(id(workspace))

        by_visible = cached[1]
        filtered = by_visible.get(visible_locations)
        if filtered is None:
            filtered = type(workspace)(
                code_location_entries={
                    name: entry
                    for name, entry in workspace.code_location_entries.items()
                    if name.lower() in visible_locations
                }
            )
            by_visible[visible_locations] = filtered
        return filtered


# ---------------------------------------------------------------------------
# Instance
# ---------------------------------------------------------------------------

_label_cache: "weakref.WeakKeyDictionary[Any, tuple]" = weakref.WeakKeyDictionary()
_label_cache_lock = threading.Lock()


def _stored_repository_labels(instance: Any) -> Set[str]:
    """All ``.dagster/repository`` values in run storage (cached briefly)."""
    from dagster._core.storage.tags import REPOSITORY_LABEL_TAG

    now = time.monotonic()
    with _label_cache_lock:
        cached = _label_cache.get(instance)
        if cached is not None and now - cached[1] < _LABEL_CACHE_TTL:
            return cached[0]

    labels: Set[str] = set()
    for _, values in instance.get_run_tags(tag_keys=[REPOSITORY_LABEL_TAG]):
        labels.update(values)
    with _label_cache_lock:
        _label_cache[instance] = (labels, now)
    return labels


def _label_location(label: str) -> str:
    """Location part of a ``repo@location`` repository label."""
    return label.rsplit("@", 1)[-1]


class _Visibility:
    """What one request may see. Shared by the scoped instance and log manager."""

    def __init__(
        self,
        base_instance: Any,
        visible_locations: FrozenSet[str],
        workspace: Any,
    ):
        self.base_instance = base_instance
        self.visible_locations = visible_locations
        self.workspace = workspace
        self._run_visible: Dict[str, bool] = {}
        self._asset_keys: Optional[Set[Any]] = None
        self._labels: Optional[list] = None

    def location_visible(self, location_name: Optional[str]) -> bool:
        return location_name is not None and location_name.lower() in self.visible_locations

    def repository_labels(self) -> list:
        """Visible ``repo@location`` labels: from loaded repos and from stored runs."""
        if self._labels is None:
            labels = {
                label
                for label in _stored_repository_labels(self.base_instance)
                if self.location_visible(_label_location(label))
            }
            for entry in self.workspace.code_location_entries.values():
                if entry.code_location:
                    for repo in entry.code_location.get_repositories().values():
                        labels.add(repo.get_remote_origin().get_label())
            self._labels = sorted(labels) or [_NO_VISIBLE_LOCATION]
        return self._labels

    def run_visible(self, run: Any) -> bool:
        from dagster._core.storage.tags import REPOSITORY_LABEL_TAG

        origin = getattr(run, "remote_job_origin", None)
        if origin is not None:
            return self.location_visible(origin.location_name)
        label = (run.tags or {}).get(REPOSITORY_LABEL_TAG)
        return label is not None and self.location_visible(_label_location(label))

    def run_id_visible(self, run_id: Any) -> bool:
        """Hidden only if the run exists in a hidden location (unknown ids pass through)."""
        if not isinstance(run_id, str):
            return True
        if run_id not in self._run_visible:
            run = self.base_instance.get_run_by_id(run_id)
            self._run_visible[run_id] = run is None or self.run_visible(run)
        return self._run_visible[run_id]

    def asset_keys(self) -> Set[Any]:
        if self._asset_keys is None:
            self._asset_keys = set(self.workspace.asset_graph.get_all_asset_keys())
        return self._asset_keys

    def runs_filter(self, filters: Any) -> Any:
        """Narrow a RunsFilter to runs in visible locations."""
        from dagster._core.storage.dagster_run import RunsFilter
        from dagster._core.storage.tags import REPOSITORY_LABEL_TAG
        from dagster._record import copy as record_copy

        allowed = self.repository_labels()
        filters = filters or RunsFilter()
        requested = filters.tags.get(REPOSITORY_LABEL_TAG)
        if requested is not None:
            requested = [requested] if isinstance(requested, str) else list(requested)
            allowed = [label for label in requested if label in allowed] or [_NO_VISIBLE_LOCATION]
        return record_copy(filters, tags={**filters.tags, REPOSITORY_LABEL_TAG: allowed})

    def backfill_visible(self, backfill: Any) -> bool:
        if backfill.is_asset_backfill:
            keys = backfill.asset_selection or []
            return bool(keys) and all(key in self.asset_keys() for key in keys)
        origin = backfill.partition_set_origin
        return origin is not None and self.location_visible(origin.selector.location_name)

    def instigator_state_visible(self, state: Any) -> bool:
        origin = state.origin
        return self.location_visible(origin.repository_origin.code_location_origin.location_name)


def _asset_key_of(records_filter: Any) -> Any:
    """Asset key from an ``AssetKey`` or ``AssetRecordsFilter`` argument."""
    return getattr(records_filter, "asset_key", records_filter)


def _empty_records(cursor: Any) -> Any:
    from dagster._core.event_api import EventRecordsResult

    return EventRecordsResult(records=[], cursor=cursor or "", has_more=False)


def _first_arg(args: tuple, kwargs: dict, name: str) -> Any:
    return args[0] if args else kwargs.get(name)


_instance_classes: Dict[type, Type] = {}
_instance_classes_lock = threading.Lock()


def _scoped_instance_class(base: type) -> Type:
    """Return (and cache) a subclass of ``base`` whose reads honour ``_authkit_visibility``."""
    with _instance_classes_lock:
        scoped = _instance_classes.get(base)
        if scoped is not None:
            return scoped

        class VisibilityScoped(base):  # type: ignore[valid-type, misc]
            _authkit_visibility: _Visibility

            # --- Runs: filtered in the storage query ---

            def get_run_records(self, filters=None, *args, **kwargs):
                return super().get_run_records(
                    self._authkit_visibility.runs_filter(filters), *args, **kwargs
                )

            def get_runs(self, filters=None, *args, **kwargs):
                return super().get_runs(
                    self._authkit_visibility.runs_filter(filters), *args, **kwargs
                )

            def get_run_ids(self, filters=None, *args, **kwargs):
                return super().get_run_ids(
                    self._authkit_visibility.runs_filter(filters), *args, **kwargs
                )

            def get_runs_count(self, filters=None):
                return super().get_runs_count(self._authkit_visibility.runs_filter(filters))

            def get_run_tags(self, *args, **kwargs):
                # Drop location tag values (used by the run filter typeahead)
                # for hidden locations. Other tag values are not filtered.
                from dagster._core.storage import tags

                code_location_tag = getattr(tags, "CODE_LOCATION_TAG", None)  # Dagster 1.11+
                visibility = self._authkit_visibility
                result = []
                for key, values in super().get_run_tags(*args, **kwargs):
                    if key == tags.REPOSITORY_LABEL_TAG:
                        values = {
                            v for v in values if visibility.location_visible(_label_location(v))
                        }
                    elif key == code_location_tag:
                        values = {v for v in values if visibility.location_visible(v)}
                    if values:
                        result.append((key, values))
                return result

            # --- Runs by id ---

            def get_run_by_id(self, run_id):
                run = super().get_run_by_id(run_id)
                return run if run is None or self._authkit_visibility.run_visible(run) else None

            def get_run_record_by_id(self, run_id):
                record = super().get_run_record_by_id(run_id)
                if record is None or self._authkit_visibility.run_visible(record.dagster_run):
                    return record
                return None

            def get_run_group(self, run_id):
                if not self._authkit_visibility.run_id_visible(run_id):
                    return None
                group = super().get_run_group(run_id)
                if group is None:
                    return None
                root_run_id, runs = group
                return root_run_id, [r for r in runs if self._authkit_visibility.run_visible(r)]

            # --- Run event logs ---

            def all_logs(self, *args, **kwargs):
                if not self._authkit_visibility.run_id_visible(_first_arg(args, kwargs, "run_id")):
                    return []
                return super().all_logs(*args, **kwargs)

            def logs_after(self, *args, **kwargs):
                if not self._authkit_visibility.run_id_visible(_first_arg(args, kwargs, "run_id")):
                    return []
                return super().logs_after(*args, **kwargs)

            def get_records_for_run(self, *args, **kwargs):
                if not self._authkit_visibility.run_id_visible(_first_arg(args, kwargs, "run_id")):
                    from dagster._core.storage.event_log.base import EventLogConnection

                    cursor = args[1] if len(args) > 1 else kwargs.get("cursor")
                    return EventLogConnection(records=[], cursor=cursor or "", has_more=False)
                return super().get_records_for_run(*args, **kwargs)

            def watch_event_logs(self, *args, **kwargs):
                if not self._authkit_visibility.run_id_visible(_first_arg(args, kwargs, "run_id")):
                    return None
                return super().watch_event_logs(*args, **kwargs)

            @property
            def compute_log_manager(self):
                return _ScopedComputeLogManager(
                    super().compute_log_manager, self._authkit_visibility
                )

            # --- Backfills ---

            def get_backfills(self, *args, **kwargs):
                return [
                    b
                    for b in super().get_backfills(*args, **kwargs)
                    if self._authkit_visibility.backfill_visible(b)
                ]

            def get_backfill(self, backfill_id):
                backfill = super().get_backfill(backfill_id)
                if backfill is None or self._authkit_visibility.backfill_visible(backfill):
                    return backfill
                return None

            # --- Schedule / sensor state ---

            def all_instigator_state(self, *args, **kwargs):
                return [
                    s
                    for s in super().all_instigator_state(*args, **kwargs)
                    if self._authkit_visibility.instigator_state_visible(s)
                ]

            # --- Assets ---

            def get_asset_keys(self, *args, **kwargs):
                visible = self._authkit_visibility.asset_keys()
                return [k for k in super().get_asset_keys(*args, **kwargs) if k in visible]

            def fetch_materializations(self, records_filter, *args, **kwargs):
                return self._authkit_fetch("fetch_materializations", records_filter, args, kwargs)

            def fetch_observations(self, records_filter, *args, **kwargs):
                return self._authkit_fetch("fetch_observations", records_filter, args, kwargs)

            def fetch_failed_materializations(self, records_filter, *args, **kwargs):
                return self._authkit_fetch(
                    "fetch_failed_materializations", records_filter, args, kwargs
                )

            def get_latest_planned_materialization_info(self, asset_key, *args, **kwargs):
                if asset_key not in self._authkit_visibility.asset_keys():
                    return None
                return super().get_latest_planned_materialization_info(asset_key, *args, **kwargs)

            def _authkit_fetch(self, method, records_filter, args, kwargs):
                if _asset_key_of(records_filter) not in self._authkit_visibility.asset_keys():
                    cursor = args[1] if len(args) > 1 else kwargs.get("cursor")
                    return _empty_records(cursor)
                return getattr(super(), method)(records_filter, *args, **kwargs)

        VisibilityScoped.__name__ = VisibilityScoped.__qualname__ = (
            f"VisibilityScoped{base.__name__}"
        )
        _instance_classes[base] = VisibilityScoped
        return VisibilityScoped


class _ScopedComputeLogManager:
    """
    Delegating wrapper around the compute log manager. Log keys start with the
    run id (``[run_id, "compute_logs", step_key]``). For a hidden run's logs,
    reads return empty logs and any other call raises ``RunNotVisibleError``.
    """

    def __init__(self, manager: Any, visibility: _Visibility):
        self._manager = manager
        self._visibility = visibility

    def _hidden(self, log_key: Any) -> bool:
        return (
            isinstance(log_key, (list, tuple))
            and bool(log_key)
            and not self._visibility.run_id_visible(log_key[0])
        )

    def get_log_data(self, log_key, *args, **kwargs):
        if self._hidden(log_key):
            from dagster._core.storage.compute_log_manager import CapturedLogData

            return CapturedLogData(log_key=log_key)
        return self._manager.get_log_data(log_key, *args, **kwargs)

    def get_log_metadata(self, log_key, *args, **kwargs):
        if self._hidden(log_key):
            from dagster._core.storage.compute_log_manager import CapturedLogMetadata

            return CapturedLogMetadata()
        return self._manager.get_log_metadata(log_key, *args, **kwargs)

    def __getattr__(self, name: str) -> Any:
        attr = getattr(self._manager, name)
        if not callable(attr):
            return attr

        def checked(*args, **kwargs):
            log_key = args[0] if args else kwargs.get("log_key", kwargs.get("log_key_prefix"))
            if self._hidden(log_key):
                raise RunNotVisibleError(f"Run {log_key[0]} not found")
            return attr(*args, **kwargs)

        return checked


def scoped_instance(instance: Any, visible_locations: FrozenSet[str], workspace: Any) -> Any:
    """
    Per-request copy of ``instance`` that hides other locations' records.

    The copy shares all storage objects with the original (shallow ``__dict__``
    copy), so writes and caches behave exactly as before. ``DagsterInstance``
    has no ``__del__``; storages are only disposed explicitly, so dropping the
    copy at the end of the request is safe.
    """
    scoped = object.__new__(_scoped_instance_class(type(instance)))
    scoped.__dict__.update(instance.__dict__)
    scoped._authkit_visibility = _Visibility(instance, visible_locations, workspace)
    return scoped
