"""
End-to-end test for location-scoped RBAC.

Runs a real DagsterWebserver (patched by apply_patches) with two in-process
code locations, team_a and team_b, and sends real GraphQL through the full
ASGI stack: AuthKit middleware → Dagster GraphQL → Dagster's own permission
checks on the scoped request context.
"""

import asyncio
import json
import sys
from pathlib import Path
from unittest.mock import patch

import pytest
from dagster._core.instance_for_test import instance_for_test

try:
    from dagster._core.remote_origin import InProcessCodeLocationOrigin
except ImportError:  # Dagster < 1.11
    from dagster._core.remote_representation.origin import InProcessCodeLocationOrigin
from dagster._core.storage import tags as dagster_tags
from dagster._core.storage.compute_log_manager import ComputeIOType
from dagster._core.storage.dagster_run import DagsterRunStatus
from dagster._core.test_utils import create_run_for_test
from dagster._core.types.loadable_target_origin import LoadableTargetOrigin
from dagster._core.workspace.context import WorkspaceProcessContext
from dagster._core.workspace.load_target import InProcessWorkspaceLoadTarget

from dagster_authkit_plus.auth.backends.base import AuthUser, Role
from dagster_authkit_plus.auth.session import sessions
from dagster_authkit_plus.utils.config import config
from tests.test_location_rbac import _restore_patch_targets, _save_patch_targets

DEFS_FILE = str(Path(__file__).with_name("location_rbac_defs.py"))
REPO = "__repository__"

LAUNCH_RUN = """
mutation($params: ExecutionParams!) {
  launchRun(executionParams: $params) {
    __typename
    ... on PythonError { message }
  }
}
"""

START_SCHEDULE = """
mutation($selector: ScheduleSelector!) {
  startSchedule(scheduleSelector: $selector) { __typename }
}
"""

TERMINATE_RUN = """
mutation($runId: String!) {
  terminateRun(runId: $runId) {
    __typename
    ... on TerminateRunFailure { message }
  }
}
"""

LOCATION_PERMISSIONS = """
{
  workspaceOrError {
    ... on Workspace {
      locationEntries { name permissions { permission value disabledReason } }
    }
  }
}
"""


def _origin(location_name, attribute):
    return InProcessCodeLocationOrigin(
        LoadableTargetOrigin(
            executable_path=sys.executable, python_file=DEFS_FILE, attribute=attribute
        ),
        location_name=location_name,
    )


@pytest.fixture
def server():
    """A patched DagsterWebserver ASGI app plus its instance and workspace."""
    yield from _server(visibility="all")


@pytest.fixture
def granted_server():
    """Same as ``server``, with DAGSTER_AUTH_LOCATION_VISIBILITY=granted."""
    yield from _server(visibility="granted")


def _server(visibility):
    import dagster_webserver.webserver as webserver_module

    from dagster_authkit_plus.core.patch import apply_patches

    cls = webserver_module.DagsterWebserver
    saved = _save_patch_targets(cls)
    cls._authkit_patched = False

    with (
        patch.object(config, "DAGSTER_AUTH_LOCATION_GROUP_PATTERN", "dagster:{location}:{role}"),
        patch.object(config, "DAGSTER_AUTH_LOCATION_VISIBILITY", visibility),
        instance_for_test() as instance,
    ):
        apply_patches()
        target = InProcessWorkspaceLoadTarget(
            [_origin("team_a", "defs_a"), _origin("team_b", "defs_b")]
        )
        with WorkspaceProcessContext(instance, target) as process_context:
            app = cls(process_context).create_asgi_app()
            yield app, instance, process_context

    _restore_patch_targets(cls, saved)


def _graphql(app, user, query, variables=None):
    """POST a GraphQL request through the full ASGI app as ``user``."""
    body = json.dumps({"query": query, "variables": variables or {}}).encode()
    scope = {
        "type": "http",
        "http_version": "1.1",
        "method": "POST",
        "path": "/graphql",
        "raw_path": b"/graphql",
        "root_path": "",
        "scheme": "http",
        "query_string": b"",
        "headers": [
            (b"host", b"localhost"),
            (b"content-type", b"application/json"),
            (b"cookie", b"dagster_session=valid-token"),
        ],
        "server": ("localhost", 3000),
        "client": ("127.0.0.1", 12345),
    }
    messages = [{"type": "http.request", "body": body, "more_body": False}]
    sent = []

    async def receive():
        return messages.pop(0) if messages else {"type": "http.disconnect"}

    async def send(message):
        sent.append(message)

    with patch.object(sessions, "validate", return_value=user.to_dict()):
        asyncio.run(app(scope, receive, send))

    status = next(m["status"] for m in sent if m["type"] == "http.response.start")
    payload = b"".join(m.get("body", b"") for m in sent if m["type"] == "http.response.body")
    assert status == 200, payload
    return json.loads(payload)


def _launch(location, job_name):
    return {
        "params": {
            "selector": {
                "repositoryLocationName": location,
                "repositoryName": REPO,
                "jobName": job_name,
            },
            "mode": "default",
        }
    }


def _schedule(location, schedule_name):
    return {
        "selector": {
            "repositoryLocationName": location,
            "repositoryName": REPO,
            "scheduleName": schedule_name,
        }
    }


def _remote_run(instance, process_context, location, job_name):
    """Create a started (so terminable) run whose origin points at ``location``."""
    context = process_context.create_request_context()
    remote_job = context.get_code_location(location).get_repository(REPO).get_full_job(job_name)
    return create_run_for_test(
        instance,
        job_name=job_name,
        status=DagsterRunStatus.STARTED,
        remote_job_origin=remote_job.get_remote_origin(),
        job_code_origin=remote_job.get_python_origin(),
    )


TEAM_A_LAUNCHER = AuthUser("alice", Role.VIEWER, location_roles={"team_a": Role.LAUNCHER})
TEAM_A_EDITOR = AuthUser("erin", Role.VIEWER, location_roles={"team_a": Role.EDITOR})


class TestLocationScopedRBACEndToEnd:
    def test_launch_only_in_granted_location(self, server):
        app, _, _ = server
        allowed = _graphql(app, TEAM_A_LAUNCHER, LAUNCH_RUN, _launch("team_a", "job_a"))
        denied = _graphql(app, TEAM_A_LAUNCHER, LAUNCH_RUN, _launch("team_b", "job_b"))

        assert allowed["data"]["launchRun"]["__typename"] == "LaunchRunSuccess", allowed
        assert denied["data"]["launchRun"]["__typename"] == "UnauthorizedError", denied

    def test_terminate_by_run_id_respects_run_location(self, server):
        app, instance, process_context = server
        run_a = _remote_run(instance, process_context, "team_a", "job_a")
        run_b = _remote_run(instance, process_context, "team_b", "job_b")

        result_a = _graphql(app, TEAM_A_LAUNCHER, TERMINATE_RUN, {"runId": run_a.run_id})
        result_b = _graphql(app, TEAM_A_LAUNCHER, TERMINATE_RUN, {"runId": run_b.run_id})

        # Dagster reports this denial as a TerminateRunFailure, not UnauthorizedError.
        denied = "You do not have permission to terminate this run"
        assert result_a["data"]["terminateRun"].get("message") != denied, result_a
        assert result_b["data"]["terminateRun"]["message"] == denied, result_b

    def test_editor_schedule_only_in_granted_location(self, server):
        app, _, _ = server
        allowed = _graphql(app, TEAM_A_EDITOR, START_SCHEDULE, _schedule("team_a", "schedule_a"))
        denied = _graphql(app, TEAM_A_EDITOR, START_SCHEDULE, _schedule("team_b", "schedule_b"))

        assert allowed["data"]["startSchedule"]["__typename"] == "ScheduleStateResult", allowed
        assert denied["data"]["startSchedule"]["__typename"] == "UnauthorizedError", denied

    def test_launcher_cannot_start_schedule_in_own_location(self, server):
        app, _, _ = server
        result = _graphql(app, TEAM_A_LAUNCHER, START_SCHEDULE, _schedule("team_a", "schedule_a"))
        # Blocked by the middleware (EDITOR required, highest role is LAUNCHER).
        assert "Access Denied" in result["errors"][0]["message"], result

    def test_ui_permissions_reflect_location_roles(self, server):
        app, _, _ = server
        result = _graphql(app, TEAM_A_LAUNCHER, LOCATION_PERMISSIONS)
        entries = {
            entry["name"]: {p["permission"]: p for p in entry["permissions"]}
            for entry in result["data"]["workspaceOrError"]["locationEntries"]
        }

        assert entries["team_a"]["launch_pipeline_execution"]["value"] is True
        assert entries["team_b"]["launch_pipeline_execution"]["value"] is False
        assert entries["team_b"]["launch_pipeline_execution"]["disabledReason"] == (
            "Requires LAUNCHER role"
        )
        assert entries["team_a"]["start_schedule"]["value"] is False

    def test_global_admin_unaffected(self, server):
        app, _, _ = server
        admin = AuthUser("root", Role.ADMIN)
        result = _graphql(app, admin, LAUNCH_RUN, _launch("team_b", "job_b"))
        assert result["data"]["launchRun"]["__typename"] == "LaunchRunSuccess", result


# ---------------------------------------------------------------------------
# Read visibility (DAGSTER_AUTH_LOCATION_VISIBILITY=granted)
# ---------------------------------------------------------------------------

LOCATION_NAMES = "{ workspaceOrError { ... on Workspace { locationEntries { name } } } }"

RUNS = "{ runsOrError { ... on Runs { results { runId } count } } }"

RUN = """
query($runId: ID!) {
  runOrError(runId: $runId) { __typename }
}
"""

RUN_LOGS = """
query($runId: ID!) {
  logsForRun(runId: $runId) {
    __typename
    ... on EventConnection { events { __typename } }
  }
}
"""

ASSET_KEYS = "{ assetsOrError { ... on AssetConnection { nodes { key { path } } } } }"

ASSET_EVENTS = """
query($path: [String!]!) {
  assetOrError(assetKey: {path: $path}) {
    __typename
    ... on Asset { assetMaterializations { runId } }
  }
}
"""

TEAM_A_VIEWER = AuthUser(
    "vic", Role.VIEWER, location_roles={"team_a": Role.VIEWER}, global_access=False
)
NO_GROUPS = AuthUser("nobody", Role.VIEWER, global_access=False)
GLOBAL_VIEWER = AuthUser("gina", Role.VIEWER)


def _location_names(app, user):
    result = _graphql(app, user, LOCATION_NAMES)
    return sorted(e["name"] for e in result["data"]["workspaceOrError"]["locationEntries"])


def _visible_run_ids(app, user):
    result = _graphql(app, user, RUNS)
    runs = result["data"]["runsOrError"]
    return {r["runId"] for r in runs["results"]}, runs["count"]


def _materialize(instance, asset_name):
    from dagster import AssetKey, AssetMaterialization

    instance.report_runless_asset_event(AssetMaterialization(asset_key=AssetKey(asset_name)))


class TestLocationVisibilityEndToEnd:
    def test_workspace_shows_only_granted_locations(self, granted_server):
        app, _, _ = granted_server
        assert _location_names(app, TEAM_A_VIEWER) == ["team_a"]
        assert _location_names(app, NO_GROUPS) == []
        assert _location_names(app, GLOBAL_VIEWER) == ["team_a", "team_b"]

    def test_run_list_and_count_hide_other_locations(self, granted_server):
        app, instance, process_context = granted_server
        run_a = _remote_run(instance, process_context, "team_a", "job_a")
        run_b = _remote_run(instance, process_context, "team_b", "job_b")

        assert _visible_run_ids(app, TEAM_A_VIEWER) == ({run_a.run_id}, 1)
        assert _visible_run_ids(app, NO_GROUPS) == (set(), 0)
        assert _visible_run_ids(app, GLOBAL_VIEWER) == ({run_a.run_id, run_b.run_id}, 2)

    def test_hidden_run_not_found_by_id(self, granted_server):
        app, instance, process_context = granted_server
        run_a = _remote_run(instance, process_context, "team_a", "job_a")
        run_b = _remote_run(instance, process_context, "team_b", "job_b")

        visible = _graphql(app, TEAM_A_VIEWER, RUN, {"runId": run_a.run_id})
        hidden = _graphql(app, TEAM_A_VIEWER, RUN, {"runId": run_b.run_id})

        assert visible["data"]["runOrError"]["__typename"] == "Run"
        assert hidden["data"]["runOrError"]["__typename"] == "RunNotFoundError"

    def test_hidden_run_logs_empty(self, granted_server):
        app, instance, process_context = granted_server
        run_b = _remote_run(instance, process_context, "team_b", "job_b")
        instance.report_engine_event("hello", run_b)

        hidden = _graphql(app, TEAM_A_VIEWER, RUN_LOGS, {"runId": run_b.run_id})
        shown = _graphql(app, GLOBAL_VIEWER, RUN_LOGS, {"runId": run_b.run_id})

        assert shown["data"]["logsForRun"]["events"], shown
        hidden_logs = hidden["data"]["logsForRun"]
        assert hidden_logs["__typename"] != "EventConnection" or not hidden_logs["events"]

    def test_assets_from_hidden_locations_hidden(self, granted_server):
        app, instance, _ = granted_server
        _materialize(instance, "asset_a")
        _materialize(instance, "asset_b")

        keys = _graphql(app, TEAM_A_VIEWER, ASSET_KEYS)["data"]["assetsOrError"]["nodes"]
        assert [k["key"]["path"] for k in keys] == [["asset_a"]]

        hidden = _graphql(app, TEAM_A_VIEWER, ASSET_EVENTS, {"path": ["asset_b"]})
        shown = _graphql(app, GLOBAL_VIEWER, ASSET_EVENTS, {"path": ["asset_b"]})
        assert shown["data"]["assetOrError"]["assetMaterializations"]
        hidden_asset = hidden["data"]["assetOrError"]
        assert hidden_asset["__typename"] != "Asset" or not hidden_asset["assetMaterializations"]

    def test_hidden_location_cannot_be_launched(self, granted_server):
        app, _, _ = granted_server
        user = AuthUser(
            "lena", Role.VIEWER, location_roles={"team_a": Role.LAUNCHER}, global_access=False
        )
        allowed = _graphql(app, user, LAUNCH_RUN, _launch("team_a", "job_a"))
        hidden = _graphql(app, user, LAUNCH_RUN, _launch("team_b", "job_b"))

        assert allowed["data"]["launchRun"]["__typename"] == "LaunchRunSuccess", allowed
        assert hidden["data"]["launchRun"]["__typename"] != "LaunchRunSuccess", hidden

    def test_visibility_all_shows_everything(self, server):
        app, _, _ = server
        assert _location_names(app, TEAM_A_VIEWER) == ["team_a", "team_b"]


RUNS_FEED = """
{
  runsFeedOrError(limit: 20, view: ROOTS) {
    ... on RunsFeedConnection { results { id } }
  }
}
"""

CAPTURED_LOGS = """
query($logKey: [String!]!) {
  capturedLogs(logKey: $logKey) { stdout }
}
"""

RUN_TAG_VALUES = """
{
  runTagsOrError(tagKeys: ["dagster/code_location"]) {
    ... on RunTags { tags { key values } }
  }
}
"""


class TestLocationVisibilityOtherReads:
    def test_runs_feed_hides_other_locations(self, granted_server):
        app, instance, process_context = granted_server
        run_a = _remote_run(instance, process_context, "team_a", "job_a")
        _remote_run(instance, process_context, "team_b", "job_b")

        feed = _graphql(app, TEAM_A_VIEWER, RUNS_FEED)["data"]["runsFeedOrError"]["results"]
        assert [entry["id"] for entry in feed] == [run_a.run_id]

    def test_captured_logs_of_hidden_run_empty(self, granted_server):
        app, instance, process_context = granted_server
        run_a = _remote_run(instance, process_context, "team_a", "job_a")
        run_b = _remote_run(instance, process_context, "team_b", "job_b")

        manager = instance.compute_log_manager
        with manager.open_log_stream([run_b.run_id, "x"], ComputeIOType.STDOUT) as stream:
            stream.write("secret output")

        shown = _graphql(app, GLOBAL_VIEWER, CAPTURED_LOGS, {"logKey": [run_b.run_id, "x"]})
        hidden = _graphql(app, TEAM_A_VIEWER, CAPTURED_LOGS, {"logKey": [run_b.run_id, "x"]})
        own = _graphql(app, TEAM_A_VIEWER, CAPTURED_LOGS, {"logKey": [run_a.run_id, "x"]})

        assert "secret output" in shown["data"]["capturedLogs"]["stdout"]
        assert not hidden["data"]["capturedLogs"]["stdout"]
        assert "errors" not in own, own

    def test_schedule_state_of_hidden_location_hidden(self, granted_server):
        from dagster_authkit_plus.core.location_visibility import filter_workspace, scoped_instance

        app, instance, process_context = granted_server
        admin = AuthUser("root", Role.ADMIN)
        _graphql(app, admin, START_SCHEDULE, _schedule("team_a", "schedule_a"))
        _graphql(app, admin, START_SCHEDULE, _schedule("team_b", "schedule_b"))

        visible = frozenset({"team_a"})
        workspace = filter_workspace(process_context.get_current_workspace(), visible)
        scoped = scoped_instance(instance, visible, workspace)

        assert sorted(s.name for s in instance.all_instigator_state()) == [
            "schedule_a",
            "schedule_b",
        ]
        assert [s.name for s in scoped.all_instigator_state()] == ["schedule_a"]

    @pytest.mark.skipif(
        not hasattr(dagster_tags, "CODE_LOCATION_TAG"),
        reason="dagster/code_location run tag was added in Dagster 1.12",
    )
    def test_run_tag_values_hide_other_locations(self, granted_server):
        app, instance, process_context = granted_server
        _remote_run(instance, process_context, "team_a", "job_a")
        _remote_run(instance, process_context, "team_b", "job_b")

        tags = _graphql(app, TEAM_A_VIEWER, RUN_TAG_VALUES)["data"]["runTagsOrError"]["tags"]
        assert tags == [{"key": "dagster/code_location", "values": ["team_a"]}]
