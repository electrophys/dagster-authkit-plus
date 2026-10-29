"""
Location-scoped RBAC via Dagster's own permission checks.

Dagster's GraphQL resolvers already ask the request context for permissions
per code location (``has_permission_for_location``, ``_for_run``,
``_for_selector``, ``_for_backfill``). In OSS every location gets the same
answer. This module wraps the per-request ``WorkspaceRequestContext`` so the
answers reflect the current user's role in each location:

- ``permissions`` (global) — enabled only if the user's *global* role allows it.
  Dagster's selector checks short-circuit to True when the global permission is
  enabled, so this map must stay restrictive.
- ``permissions_for_location`` — enabled if the user's role *in that location*
  allows it.

The Dagster UI reads the same permissions, so buttons for locations the user
can't act on are disabled rather than failing on click.

Read access is NOT scoped: every authenticated user can still see all locations.
"""

import logging
from typing import Any, Dict, Mapping, Optional, Type

from dagster_authkit.auth.backends.base import AuthUser, Role

logger = logging.getLogger(__name__)

# Minimum role for each Dagster ``Permissions`` value, keyed by the enum's
# string value so this module doesn't import Dagster at load time.
# Mirrors the mutation lists in RolePermissions. Permissions missing here
# (added in a newer Dagster) fall back to DAGSTER_AUTH_UNKNOWN_MUTATION_ROLE.
PERMISSION_ROLES: Dict[str, Role] = {
    # LAUNCHER — runs
    "launch_pipeline_execution": Role.LAUNCHER,
    "launch_pipeline_reexecution": Role.LAUNCHER,
    "terminate_pipeline_execution": Role.LAUNCHER,
    "delete_pipeline_run": Role.LAUNCHER,
    # EDITOR — schedules, sensors, assets, backfills, partitions, concurrency
    "start_schedule": Role.EDITOR,
    "stop_running_schedule": Role.EDITOR,
    "schedule_dry_run": Role.EDITOR,
    "edit_sensor": Role.EDITOR,
    "sensor_dry_run": Role.EDITOR,
    "update_sensor_cursor": Role.EDITOR,
    "wipe_assets": Role.EDITOR,
    "report_runless_asset_events": Role.EDITOR,
    "launch_partition_backfill": Role.EDITOR,
    "cancel_partition_backfill": Role.EDITOR,
    "edit_dynamic_partitions": Role.EDITOR,
    "toggle_auto_materialize": Role.EDITOR,
    "edit_concurrency_limit": Role.EDITOR,
    # ADMIN — code locations, workspace, components
    "reload_repository_location": Role.ADMIN,
    "reload_workspace": Role.ADMIN,
    "edit_app_managed_components": Role.ADMIN,
    "refresh_component_state": Role.ADMIN,
}


def required_role_for_permission(permission: str, default_role: Role) -> Role:
    """Minimum role for a Dagster permission; ``default_role`` if unknown."""
    return PERMISSION_ROLES.get(str(permission), default_role)


def get_connection_user(conn: Any) -> Optional[AuthUser]:
    """
    Return the AuthUser the middleware stored on a Starlette connection.

    The middleware stores a ``State`` object, but ``scope["state"]`` can also be
    a plain dict depending on the Starlette version, so both are checked.
    """
    raw_state = conn.scope.get("state")
    if isinstance(raw_state, dict):
        user = raw_state.get("user")
    else:
        user = getattr(raw_state, "user", None)
    return user if isinstance(user, AuthUser) else None


def request_context_method_name(webserver_class: type) -> Optional[str]:
    """
    Name of the DagsterWebserver method that builds the per-request context:
    ``_make_request_context`` (Dagster 1.12+) or ``make_request_context`` (older).
    """
    for name in ("_make_request_context", "make_request_context"):
        if hasattr(webserver_class, name):
            return name
    return None


def _restrict(
    permissions: Mapping[str, Any], role: Optional[Role], default_role: Role
) -> Dict[str, Any]:
    """Disable every permission ``role`` doesn't reach. ``None`` disables all."""
    restricted = {}
    for permission, result in permissions.items():
        required = required_role_for_permission(permission, default_role)
        if result.enabled and (role is None or role < required):
            result = type(result)(enabled=False, disabled_reason=f"Requires {required.name} role")
        restricted[permission] = result
    return restricted


_scoped_classes: Dict[type, Type] = {}


def _scoped_class(base: type) -> Type:
    """
    Return (and cache) a subclass of ``base`` that narrows the permissions
    ``base`` grants to what the user's roles allow.

    The subclass derives from ``base`` alone (no mixin): CPython only allows
    ``__class__`` assignment when the first base has the same instance layout.
    """
    scoped = _scoped_classes.get(base)
    if scoped is not None:
        return scoped

    class LocationScoped(base):  # type: ignore[valid-type, misc]
        _authkit_scoped = True
        _authkit_user: Optional[AuthUser]
        _authkit_default_role: Role

        @property
        def permissions(self) -> Mapping[str, Any]:
            user = self._authkit_user
            return _restrict(
                super().permissions, user.role if user else None, self._authkit_default_role
            )

        def permissions_for_location(self, *, location_name: str) -> Mapping[str, Any]:
            user = self._authkit_user
            role = user.role_for_location(location_name) if user else None
            return _restrict(
                super().permissions_for_location(location_name=location_name),
                role,
                self._authkit_default_role,
            )

        # Read visibility (only set when locations are hidden from the user)

        @property
        def instance(self) -> Any:
            scoped = self.__dict__.get("_authkit_instance")
            return scoped if scoped is not None else super().instance

        def get_current_workspace(self) -> Any:
            scoped = self.__dict__.get("_authkit_workspace")
            return scoped if scoped is not None else super().get_current_workspace()

    LocationScoped.__name__ = LocationScoped.__qualname__ = f"LocationScoped{base.__name__}"
    _scoped_classes[base] = LocationScoped
    return LocationScoped


def scope_request_context(
    context: Any,
    user: Optional[AuthUser],
    default_role: Role,
    restrict_visibility: bool = False,
) -> Any:
    """
    Apply the user's roles to a Dagster request context, in place.

    The context's class is swapped for a subclass that overrides
    ``permissions`` and ``permissions_for_location``. Swapping the class
    (rather than wrapping the object) keeps Dagster's own methods, such as
    ``has_permission_for_selector``, calling the overridden versions.

    With ``restrict_visibility``, users without global access (or no user at
    all) also get a filtered workspace and instance; see ``location_visibility``.

    Args:
        context:             Context returned by ``DagsterWebserver._make_request_context``.
        user:                Authenticated user, or ``None`` to deny every permission.
        default_role:        Required role for permissions not in ``PERMISSION_ROLES``.
        restrict_visibility: ``DAGSTER_AUTH_LOCATION_VISIBILITY=granted``.

    Returns:
        The same context object.
    """
    if user is None:
        logger.warning("No authenticated user on request context; denying all permissions")
    if not getattr(type(context), "_authkit_scoped", False):
        context.__class__ = _scoped_class(type(context))
    context._authkit_user = user
    context._authkit_default_role = default_role

    if restrict_visibility and (user is None or not user.global_access):
        _restrict_visibility(context, frozenset(user.location_roles) if user else frozenset())
    else:
        _unrestrict_visibility(context)
    return context


def _restrict_visibility(context: Any, visible_locations: frozenset) -> None:
    """Give the context a workspace and instance that only show visible locations."""
    from dagster_authkit.core.location_visibility import filter_workspace, scoped_instance

    # Keep the originals so scoping the same context again starts from them.
    if "_authkit_base_workspace" not in context.__dict__:
        context._authkit_base_workspace = context.get_current_workspace()
        context._authkit_base_instance = context.instance
    base_workspace = context._authkit_base_workspace
    base_instance = context._authkit_base_instance

    workspace = filter_workspace(base_workspace, visible_locations)
    context._authkit_workspace = workspace
    context._authkit_instance = scoped_instance(base_instance, visible_locations, workspace)
    _set_private_attrs(context, workspace, context._authkit_instance)


def _unrestrict_visibility(context: Any) -> None:
    """Undo ``_restrict_visibility`` (only matters if a context is scoped twice)."""
    context.__dict__.pop("_authkit_workspace", None)
    context.__dict__.pop("_authkit_instance", None)
    if "_authkit_base_workspace" in context.__dict__:
        _set_private_attrs(context, context._authkit_base_workspace, context._authkit_base_instance)


def _set_private_attrs(context: Any, workspace: Any, instance: Any) -> None:
    # WorkspaceRequestContext reads these attributes directly in some methods.
    if "_current_workspace" in context.__dict__:
        context._current_workspace = workspace
    if "_instance" in context.__dict__:
        context._instance = instance
