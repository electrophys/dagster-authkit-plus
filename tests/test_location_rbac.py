"""
Unit tests for location-scoped RBAC.

Covers:
- auth/location_roles.py: pattern compilation and group parsing
- AuthUser location roles: role_for_location, highest_role, (de)serialization
- ProxyAuthBackend: location roles from the groups header
- core/location_rbac.py: scoping a real Dagster WorkspaceRequestContext,
  including Dagster's own selector/location permission helpers
- PERMISSION_ROLES covers every Permission in the installed Dagster
- Middleware: location-scoped mutations use the highest location role
- apply_patches: request-context method patched only when enabled
"""

import json
from unittest.mock import MagicMock, patch

import pytest
from dagster._core.definitions.selector import JobSelector
from dagster._core.workspace.context import WorkspaceRequestContext
from dagster._core.workspace.permissions import Permissions
from starlette.datastructures import State

from dagster_authkit_plus.auth.backends.base import AuthUser, Role
from dagster_authkit_plus.auth.backends.proxy import ProxyAuthBackend
from dagster_authkit_plus.auth.location_roles import (
    compile_location_group_pattern,
    parse_location_roles,
)
from dagster_authkit_plus.core.location_rbac import (
    PERMISSION_ROLES,
    get_connection_user,
    request_context_method_name,
    scope_request_context,
)

LOCATIONS = {"team_a", "team_b"}


def _save_patch_targets(cls):
    """Snapshot the DagsterWebserver attributes apply_patches() replaces."""
    names = ("build_middleware", "build_routes", "index_html_endpoint")
    names += (request_context_method_name(cls),)
    return {name: cls.__dict__.get(name) for name in names}


def _restore_patch_targets(cls, saved):
    for name, value in saved.items():
        if value is None:
            if name in cls.__dict__:
                delattr(cls, name)  # was inherited before patching
        else:
            setattr(cls, name, value)
    cls._authkit_patched = False


# ---------------------------------------------------------------------------
# Group parsing
# ---------------------------------------------------------------------------


class TestParseLocationRoles:
    def test_colon_pattern(self):
        pattern = compile_location_group_pattern("dagster:{location}:{role}")
        roles = parse_location_roles(["dagster:team_a:launchers"], pattern)
        assert roles == {"team_a": Role.LAUNCHER}

    def test_keycloak_full_group_path(self):
        pattern = compile_location_group_pattern("/dagster/{location}/{role}")
        roles = parse_location_roles(
            ["/dagster/team_a/editors", "/dagster/team_b/viewers", "/other"], pattern
        )
        assert roles == {"team_a": Role.EDITOR, "team_b": Role.VIEWER}

    def test_singular_role_names(self):
        pattern = compile_location_group_pattern("dagster:{location}:{role}")
        roles = parse_location_roles(["dagster:team_a:admin"], pattern)
        assert roles == {"team_a": Role.ADMIN}

    def test_highest_role_wins_per_location(self):
        pattern = compile_location_group_pattern("dagster:{location}:{role}")
        roles = parse_location_roles(
            ["dagster:team_a:viewers", "dagster:team_a:editors", "dagster:team_a:launchers"],
            pattern,
        )
        assert roles == {"team_a": Role.EDITOR}

    def test_case_insensitive_and_lowercased(self):
        pattern = compile_location_group_pattern("dagster:{location}:{role}")
        roles = parse_location_roles(["DAGSTER:Team_A:Launchers"], pattern)
        assert roles == {"team_a": Role.LAUNCHER}

    def test_unknown_role_word_ignored(self):
        pattern = compile_location_group_pattern("dagster:{location}:{role}")
        assert parse_location_roles(["dagster:team_a:owners"], pattern) == {}

    def test_non_matching_groups_ignored(self):
        pattern = compile_location_group_pattern("dagster:{location}:{role}")
        assert parse_location_roles(["admins", "dagster:launchers"], pattern) == {}

    def test_regex_characters_in_pattern_are_literal(self):
        pattern = compile_location_group_pattern("dag.ster:{location}:{role}")
        assert parse_location_roles(["dagXster:team_a:admins"], pattern) == {}
        assert parse_location_roles(["dag.ster:team_a:admins"], pattern) == {"team_a": Role.ADMIN}

    @pytest.mark.parametrize(
        "bad_pattern",
        ["dagster:{location}", "dagster:{role}", "{location}:{location}:{role}"],
    )
    def test_invalid_pattern_raises(self, bad_pattern):
        with pytest.raises(ValueError, match="DAGSTER_AUTH_LOCATION_GROUP_PATTERN"):
            compile_location_group_pattern(bad_pattern)


# ---------------------------------------------------------------------------
# AuthUser
# ---------------------------------------------------------------------------


class TestAuthUserLocationRoles:
    def test_role_for_location_uses_higher_of_global_and_scoped(self):
        user = AuthUser("alice", Role.LAUNCHER, location_roles={"team_a": Role.EDITOR})
        assert user.role_for_location("team_a") == Role.EDITOR
        assert user.role_for_location("Team_A") == Role.EDITOR
        assert user.role_for_location("team_b") == Role.LAUNCHER

    def test_scoped_role_never_lowers_global_role(self):
        user = AuthUser("alice", Role.EDITOR, location_roles={"team_a": Role.VIEWER})
        assert user.role_for_location("team_a") == Role.EDITOR

    def test_highest_role(self):
        user = AuthUser("alice", Role.VIEWER, location_roles={"team_a": Role.LAUNCHER})
        assert user.highest_role == Role.LAUNCHER
        assert AuthUser("bob", Role.EDITOR).highest_role == Role.EDITOR

    def test_round_trip(self):
        user = AuthUser("alice", Role.VIEWER, location_roles={"team_a": Role.LAUNCHER})
        data = user.to_dict()
        assert data["location_roles"] == {"team_a": 20}
        assert AuthUser.from_dict(data) == user

    def test_to_dict_omits_empty_location_roles(self):
        assert "location_roles" not in AuthUser("bob", Role.VIEWER).to_dict()

    def test_from_dict_accepts_role_names(self):
        user = AuthUser.from_dict(
            {"username": "alice", "role": "VIEWER", "location_roles": {"team_a": "EDITOR"}}
        )
        assert user.location_roles == {"team_a": Role.EDITOR}


# ---------------------------------------------------------------------------
# Proxy backend
# ---------------------------------------------------------------------------


class TestProxyLocationRoles:
    @pytest.fixture
    def config(self):
        return {
            "DAGSTER_AUTH_PROXY_USER_HEADER": "X-Forwarded-Preferred-Username",
            "DAGSTER_AUTH_PROXY_GROUPS_HEADER": "X-Forwarded-Groups",
            "DAGSTER_AUTH_PROXY_EMAIL_HEADER": "X-Forwarded-Email",
            "DAGSTER_AUTH_PROXY_NAME_HEADER": "X-Forwarded-Preferred-Username",
            "DAGSTER_AUTH_LOCATION_GROUP_PATTERN": "/dagster/{location}/{role}",
        }

    def test_location_roles_from_groups_header(self, config):
        backend = ProxyAuthBackend(config)
        user = backend.get_user_from_headers(
            {
                "X-Forwarded-Preferred-Username": "alice",
                "X-Forwarded-Groups": "/dagster/team_a/launchers,/dagster/team_b/editors",
            }
        )
        assert user.role == Role.VIEWER
        assert user.location_roles == {"team_a": Role.LAUNCHER, "team_b": Role.EDITOR}

    def test_global_role_still_applies(self, config):
        backend = ProxyAuthBackend(config)
        user = backend.get_user_from_headers(
            {
                "X-Forwarded-Preferred-Username": "alice",
                "X-Forwarded-Groups": "admins,/dagster/team_a/launchers",
            }
        )
        assert user.role == Role.ADMIN
        assert user.location_roles == {"team_a": Role.LAUNCHER}

    def test_no_default_role_warning_with_location_groups(self, config, caplog):
        backend = ProxyAuthBackend(config)
        backend.get_user_from_headers(
            {
                "X-Forwarded-Preferred-Username": "alice",
                "X-Forwarded-Groups": "/dagster/team_a/launchers",
            }
        )
        assert "No group match found" not in caplog.text

    def test_disabled_without_pattern(self, config):
        del config["DAGSTER_AUTH_LOCATION_GROUP_PATTERN"]
        backend = ProxyAuthBackend(config)
        user = backend.get_user_from_headers(
            {
                "X-Forwarded-Preferred-Username": "alice",
                "X-Forwarded-Groups": "/dagster/team_a/launchers",
            }
        )
        assert user.location_roles == {}


# ---------------------------------------------------------------------------
# Scoping a real Dagster request context
# ---------------------------------------------------------------------------


class _Context(WorkspaceRequestContext):
    """Real WorkspaceRequestContext with a fixed set of code locations."""

    def has_code_location_name(self, name: str) -> bool:
        return name in LOCATIONS


def _make_context(user, read_only=False):
    context = _Context(
        instance=MagicMock(),
        current_workspace=MagicMock(),
        process_context=MagicMock(),
        version="test",
        source=None,
        read_only=read_only,
    )
    return scope_request_context(context, user, Role.ADMIN)


def _job(location):
    return JobSelector(location_name=location, repository_name="repo", job_name="job")


class TestScopedRequestContext:
    @pytest.fixture
    def team_a_launcher(self):
        return AuthUser("alice", Role.VIEWER, location_roles={"team_a": Role.LAUNCHER})

    def test_launch_allowed_only_in_granted_location(self, team_a_launcher):
        context = _make_context(team_a_launcher)
        launch = Permissions.LAUNCH_PIPELINE_EXECUTION
        assert context.has_permission_for_location(launch, "team_a")
        assert not context.has_permission_for_location(launch, "team_b")

    @pytest.mark.skipif(
        not hasattr(WorkspaceRequestContext, "has_permission_for_selector"),
        reason="has_permission_for_selector was added in Dagster 1.11",
    )
    def test_selector_check_scoped(self, team_a_launcher):
        # Dagster 1.11+ resolvers go through this helper, which returns True
        # early if the global permission is enabled.
        context = _make_context(team_a_launcher)
        launch = Permissions.LAUNCH_PIPELINE_EXECUTION
        assert context.has_permission_for_selector(launch, _job("team_a"))
        assert not context.has_permission_for_selector(launch, _job("team_b"))

    def test_global_permission_follows_global_role(self, team_a_launcher):
        context = _make_context(team_a_launcher)
        # Must stay disabled: Dagster's selector checks return True early
        # when the global permission is enabled.
        assert not context.has_permission(Permissions.LAUNCH_PIPELINE_EXECUTION)

    def test_location_role_does_not_grant_higher_permissions(self, team_a_launcher):
        context = _make_context(team_a_launcher)
        assert not context.has_permission_for_location(Permissions.EDIT_SENSOR, "team_a")
        assert not context.has_permission_for_location(
            Permissions.RELOAD_REPOSITORY_LOCATION, "team_a"
        )

    def test_unknown_location_falls_back_to_global(self, team_a_launcher):
        context = _make_context(team_a_launcher)
        assert not context.has_permission_for_location(
            Permissions.LAUNCH_PIPELINE_EXECUTION, "not_in_workspace"
        )

    def test_global_admin_gets_everything_dagster_grants(self):
        context = _make_context(AuthUser("root", Role.ADMIN))
        unscoped = _Context(
            instance=MagicMock(),
            current_workspace=MagicMock(),
            process_context=MagicMock(),
            version="test",
            source=None,
            read_only=False,
        )
        # AuthKit only narrows: Dagster OSS disables some permissions itself
        # (e.g. app-managed components), and those stay disabled.
        for permission in Permissions:
            assert context.has_permission(permission) == unscoped.has_permission(permission)
        assert context.has_permission_for_location(Permissions.RELOAD_REPOSITORY_LOCATION, "team_b")

    def test_disabled_reason_names_required_role(self, team_a_launcher):
        context = _make_context(team_a_launcher)
        result = context.permissions_for_location(location_name="team_b")[
            Permissions.LAUNCH_PIPELINE_EXECUTION
        ]
        assert not result.enabled
        assert result.disabled_reason == "Requires LAUNCHER role"

    def test_no_user_denies_everything(self):
        context = _make_context(None)
        assert not any(result.enabled for result in context.permissions.values())
        assert not any(
            result.enabled
            for result in context.permissions_for_location(location_name="team_a").values()
        )

    def test_read_only_dagster_stays_read_only(self):
        context = _make_context(AuthUser("root", Role.ADMIN), read_only=True)
        assert not context.has_permission(Permissions.LAUNCH_PIPELINE_EXECUTION)

    def test_permission_checks_are_still_recorded(self, team_a_launcher):
        # Dagster's @require_permission_check fails the request if this is False.
        context = _make_context(team_a_launcher)
        context.has_permission_for_location(Permissions.LAUNCH_PIPELINE_EXECUTION, "team_a")
        assert context.was_permission_checked(Permissions.LAUNCH_PIPELINE_EXECUTION)

    def test_scoping_twice_replaces_user(self, team_a_launcher):
        context = _make_context(team_a_launcher)
        scope_request_context(context, AuthUser("root", Role.ADMIN), Role.ADMIN)
        assert context.has_permission(Permissions.LAUNCH_PIPELINE_EXECUTION)
        assert type(context).__mro__.count(_Context) == 1

    def test_every_dagster_permission_is_mapped(self):
        unmapped = {p.value for p in Permissions} - set(PERMISSION_ROLES)
        assert not unmapped, f"Add these to PERMISSION_ROLES: {sorted(unmapped)}"


class TestGetConnectionUser:
    def test_state_object(self):
        user = AuthUser("alice", Role.VIEWER)
        state = State()
        state.user = user
        assert get_connection_user(MagicMock(scope={"state": state})) is user

    def test_plain_dict_state(self):
        user = AuthUser("alice", Role.VIEWER)
        assert get_connection_user(MagicMock(scope={"state": {"user": user}})) is user

    def test_missing_or_wrong_type(self):
        assert get_connection_user(MagicMock(scope={})) is None
        assert get_connection_user(MagicMock(scope={"state": {"user": "alice"}})) is None


# ---------------------------------------------------------------------------
# Middleware
# ---------------------------------------------------------------------------


async def _run_mutation(user, mutation_name, location_rbac):
    from dagster_authkit_plus.auth.session import sessions
    from dagster_authkit_plus.core.middleware import DagsterAuthMiddleware

    seen_scope = {}

    async def mock_app(scope, receive, send):
        seen_scope.update(scope)

    body = json.dumps({"query": f"mutation {{ {mutation_name} }}"}).encode("utf-8")

    async def receive():
        return {"type": "http.request", "body": body, "more_body": False}

    sent = []

    async def send(message):
        sent.append(message)

    middleware = DagsterAuthMiddleware(mock_app)
    middleware._location_rbac = location_rbac
    scope = {
        "type": "http",
        "method": "POST",
        "path": "/graphql",
        "headers": [(b"cookie", b"dagster_session=valid-token")],
        "query_string": b"",
        "server": ("localhost", 3000),
        "client": ("127.0.0.1", 12345),
    }
    with patch.object(sessions, "validate", return_value=user.to_dict()):
        await middleware._handle_http(scope, receive, send)

    denied = any(
        m["type"] == "http.response.body" and b"Access Denied" in m.get("body", b"") for m in sent
    )
    return seen_scope, denied


class TestMiddlewareLocationRBAC:
    @pytest.fixture
    def team_a_editor(self):
        return AuthUser("alice", Role.VIEWER, location_roles={"team_a": Role.EDITOR})

    @pytest.mark.asyncio
    async def test_location_scoped_mutation_uses_highest_role(self, team_a_editor):
        seen_scope, denied = await _run_mutation(team_a_editor, "launchRun", True)
        assert not denied
        assert seen_scope["state"].user == team_a_editor

    @pytest.mark.asyncio
    async def test_non_location_mutation_uses_global_role(self, team_a_editor):
        # Dagster only checks setConcurrencyLimit globally, so a location role
        # must not unlock it.
        _, denied = await _run_mutation(team_a_editor, "setConcurrencyLimit", True)
        assert denied

    @pytest.mark.asyncio
    async def test_disabled_uses_global_role(self, team_a_editor):
        _, denied = await _run_mutation(team_a_editor, "launchRun", False)
        assert denied

    @pytest.mark.asyncio
    async def test_websocket_scope_carries_user(self):
        from dagster_authkit_plus.auth.session import sessions
        from dagster_authkit_plus.core.middleware import DagsterAuthMiddleware

        seen_scope = {}

        async def mock_app(scope, receive, send):
            seen_scope.update(scope)

        middleware = DagsterAuthMiddleware(mock_app)
        scope = {
            "type": "websocket",
            "path": "/graphql",
            "headers": [(b"cookie", b"dagster_session=valid-token")],
            "query_string": b"",
        }
        with patch.object(sessions, "validate", return_value={"username": "bob", "role": 10}):
            await middleware._handle_websocket(scope, None, None)

        assert seen_scope["state"].user.username == "bob"


# ---------------------------------------------------------------------------
# apply_patches
# ---------------------------------------------------------------------------


class TestApplyPatches:
    @pytest.fixture
    def webserver_class(self):
        import dagster_webserver.webserver as webserver_module

        cls = webserver_module.DagsterWebserver
        saved = _save_patch_targets(cls)
        cls._authkit_patched = False
        yield cls
        _restore_patch_targets(cls, saved)

    def test_patched_when_enabled(self, webserver_class):
        from dagster_authkit_plus.core.patch import apply_patches
        from dagster_authkit_plus.utils.config import config

        method_name = request_context_method_name(webserver_class)
        original = getattr(webserver_class, method_name)
        with patch.object(
            config, "DAGSTER_AUTH_LOCATION_GROUP_PATTERN", "dagster:{location}:{role}"
        ):
            apply_patches()
        assert getattr(webserver_class, method_name) is not original

        user = AuthUser("alice", Role.VIEWER, location_roles={"team_a": Role.LAUNCHER})
        state = State()
        state.user = user
        server = MagicMock()
        server._process_context.create_request_context.return_value = _Context(
            instance=MagicMock(),
            current_workspace=MagicMock(),
            process_context=MagicMock(),
            version="test",
            source=None,
            read_only=False,
        )
        context = getattr(webserver_class, method_name)(server, MagicMock(scope={"state": state}))
        assert context.has_permission_for_location(Permissions.LAUNCH_PIPELINE_EXECUTION, "team_a")
        assert not context.has_permission_for_location(
            Permissions.LAUNCH_PIPELINE_EXECUTION, "team_b"
        )

    def test_not_patched_when_disabled(self, webserver_class):
        from dagster_authkit_plus.core.patch import apply_patches
        from dagster_authkit_plus.utils.config import config

        method_name = request_context_method_name(webserver_class)
        original = getattr(webserver_class, method_name)
        with patch.object(config, "DAGSTER_AUTH_LOCATION_GROUP_PATTERN", None):
            apply_patches()
        assert getattr(webserver_class, method_name) is original


# ---------------------------------------------------------------------------
# Read visibility (DAGSTER_AUTH_LOCATION_VISIBILITY=granted)
# ---------------------------------------------------------------------------


class TestGlobalAccess:
    def test_can_see_location(self):
        scoped = AuthUser(
            "vic", Role.VIEWER, location_roles={"team_a": Role.VIEWER}, global_access=False
        )
        assert scoped.can_see_location("Team_A")
        assert not scoped.can_see_location("team_b")
        assert AuthUser("gina", Role.VIEWER).can_see_location("team_b")

    def test_round_trip_and_default(self):
        user = AuthUser("vic", Role.VIEWER, global_access=False)
        assert user.to_dict()["global_access"] is False
        assert AuthUser.from_dict(user.to_dict()).global_access is False
        assert "global_access" not in AuthUser("gina", Role.VIEWER).to_dict()
        assert AuthUser.from_dict({"username": "old", "role": 10}).global_access is True

    @pytest.mark.parametrize(
        "groups, expected",
        [
            ("viewers,dagster:team_a:launchers", True),
            ("dagster:team_a:launchers", False),
            ("", False),
        ],
    )
    def test_proxy_sets_global_access_from_plain_groups(self, groups, expected):
        backend = ProxyAuthBackend(
            {
                "DAGSTER_AUTH_PROXY_USER_HEADER": "Remote-User",
                "DAGSTER_AUTH_PROXY_GROUPS_HEADER": "Remote-Groups",
                "DAGSTER_AUTH_PROXY_EMAIL_HEADER": "Remote-Email",
                "DAGSTER_AUTH_PROXY_NAME_HEADER": "Remote-Name",
                "DAGSTER_AUTH_LOCATION_GROUP_PATTERN": "dagster:{location}:{role}",
            }
        )
        user = backend.get_user_from_headers({"Remote-User": "u", "Remote-Groups": groups})
        assert user.global_access is expected


class TestVisibilityConfig:
    def _config(self, monkeypatch, **env):
        from dagster_authkit_plus.utils.config import AuthConfig

        for key, value in env.items():
            monkeypatch.setenv(key, value)
        return AuthConfig()

    def test_default_is_all(self, monkeypatch):
        assert self._config(monkeypatch).DAGSTER_AUTH_LOCATION_VISIBILITY == "all"

    def test_granted_requires_pattern(self, monkeypatch):
        monkeypatch.delenv("DAGSTER_AUTH_LOCATION_GROUP_PATTERN", raising=False)
        with pytest.raises(ValueError, match="requires"):
            self._config(monkeypatch, DAGSTER_AUTH_LOCATION_VISIBILITY="granted")

    def test_invalid_value(self, monkeypatch):
        with pytest.raises(ValueError, match="DAGSTER_AUTH_LOCATION_VISIBILITY"):
            self._config(monkeypatch, DAGSTER_AUTH_LOCATION_VISIBILITY="some")


class TestVisibilityRules:
    @pytest.fixture
    def visibility(self):
        from dagster._core.workspace.workspace import CurrentWorkspace

        from dagster_authkit_plus.core.location_visibility import _Visibility

        instance = MagicMock()
        instance.get_run_tags.return_value = [
            (".dagster/repository", {"repo@team_a", "repo@team_b", "other@team_a"})
        ]
        vis = _Visibility(
            instance, frozenset({"team_a"}), CurrentWorkspace(code_location_entries={})
        )
        vis._asset_keys = {"asset_a"}
        return vis

    def test_runs_filter_adds_visible_labels(self, visibility):
        from dagster._core.storage.dagster_run import RunsFilter

        narrowed = visibility.runs_filter(RunsFilter(job_name="job_a"))
        assert narrowed.job_name == "job_a"
        assert narrowed.tags[".dagster/repository"] == ["other@team_a", "repo@team_a"]

    def test_runs_filter_intersects_requested_label(self, visibility):
        from dagster._core.storage.dagster_run import RunsFilter

        shown = visibility.runs_filter(RunsFilter(tags={".dagster/repository": "repo@team_a"}))
        hidden = visibility.runs_filter(RunsFilter(tags={".dagster/repository": "repo@team_b"}))
        assert shown.tags[".dagster/repository"] == ["repo@team_a"]
        assert hidden.tags[".dagster/repository"] == ["__authkit_no_visible_location__"]

    def test_runs_filter_matches_nothing_without_visible_locations(self):
        from dagster._core.workspace.workspace import CurrentWorkspace

        from dagster_authkit_plus.core.location_visibility import _Visibility

        instance = MagicMock()
        instance.get_run_tags.return_value = [(".dagster/repository", {"repo@team_b"})]
        vis = _Visibility(instance, frozenset(), CurrentWorkspace(code_location_entries={}))
        assert vis.runs_filter(None).tags[".dagster/repository"] == [
            "__authkit_no_visible_location__"
        ]

    def test_run_without_origin_uses_label_tag(self, visibility):
        run = MagicMock(remote_job_origin=None, tags={".dagster/repository": "repo@team_a"})
        untagged = MagicMock(remote_job_origin=None, tags={})
        assert visibility.run_visible(run)
        assert not visibility.run_visible(untagged)

    def test_backfill_visibility(self, visibility):
        job_backfill = MagicMock(is_asset_backfill=False)
        job_backfill.partition_set_origin.selector.location_name = "team_b"
        assert not visibility.backfill_visible(job_backfill)
        job_backfill.partition_set_origin.selector.location_name = "team_a"
        assert visibility.backfill_visible(job_backfill)

        # Asset backfills are shown only if every asset is visible.
        assert visibility.backfill_visible(
            MagicMock(is_asset_backfill=True, asset_selection=["asset_a"])
        )
        assert not visibility.backfill_visible(
            MagicMock(is_asset_backfill=True, asset_selection=["asset_a", "asset_b"])
        )

    def test_unknown_run_id_passes_through(self, visibility):
        visibility.base_instance.get_run_by_id.return_value = None
        assert visibility.run_id_visible("does-not-exist")

    def test_filtered_workspace_is_cached(self):
        from dagster._core.workspace.workspace import CodeLocationEntry, CurrentWorkspace

        from dagster_authkit_plus.core.location_visibility import filter_workspace

        workspace = CurrentWorkspace(
            code_location_entries={
                "Team_A": MagicMock(spec=CodeLocationEntry),
                "team_b": MagicMock(spec=CodeLocationEntry),
            }
        )
        first = filter_workspace(workspace, frozenset({"team_a"}))
        assert list(first.code_location_entries) == ["Team_A"]
        assert filter_workspace(workspace, frozenset({"team_a"})) is first
        assert filter_workspace(workspace, frozenset()) is not first


class TestScopedContextVisibility:
    def test_hidden_only_for_users_without_global_access(self):
        scoped_user = AuthUser(
            "vic", Role.VIEWER, location_roles={"team_a": Role.VIEWER}, global_access=False
        )
        context = _Context(
            instance=MagicMock(),
            current_workspace=MagicMock(code_location_entries={}),
            process_context=MagicMock(),
            version="test",
            source=None,
            read_only=False,
        )
        base_instance = context.instance

        scope_request_context(context, scoped_user, Role.ADMIN, restrict_visibility=True)
        assert context.instance is not base_instance
        assert context.instance._authkit_visibility.visible_locations == {"team_a"}

        scope_request_context(context, AuthUser("gina", Role.VIEWER), Role.ADMIN, True)
        assert context.instance is base_instance

    def test_no_user_sees_nothing(self):
        context = _make_context(None)
        scope_request_context(context, None, Role.ADMIN, restrict_visibility=True)
        assert context.instance._authkit_visibility.visible_locations == frozenset()


class TestVisibilityStartupCheck:
    def test_listed_methods_match_overrides(self):
        from dagster import DagsterInstance

        from dagster_authkit_plus.core.location_visibility import (
            FILTERED_INSTANCE_METHODS,
            _scoped_instance_class,
        )

        scoped = _scoped_instance_class(DagsterInstance)
        overridden = {
            name
            for name in vars(scoped)
            if not name.startswith("_") and name != "compute_log_manager"
        } | {"compute_log_manager"}
        assert overridden == set(FILTERED_INSTANCE_METHODS)

    def test_passes_on_installed_dagster(self):
        from dagster_authkit_plus.core.detection_layer import _check_location_visibility_api

        assert _check_location_visibility_api() is None

    def test_fails_when_methods_missing(self, monkeypatch):
        import dagster

        from dagster_authkit_plus.core.detection_layer import _check_location_visibility_api

        monkeypatch.setattr(dagster, "DagsterInstance", type("RenamedInstance", (), {}))
        assert "get_backfills" in _check_location_visibility_api()
