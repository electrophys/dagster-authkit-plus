"""
Base Authentication Backend

Abstract base class that all backends must implement.
Includes Role hierarchy, AuthUser dataclass, and audit helpers.
"""

from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from enum import IntEnum
from typing import Any, Dict, List, Optional, Set

# ========================================
# ROLE HIERARCHY (Dagster+ Compatible)
# ========================================


class Role(IntEnum):
    """
    Hierarchical role levels matching Dagster+.

    Numeric values allow simple comparisons:
        user.role >= Role.LAUNCHER

    Hierarchy:
        VIEWER (10)   - Read-only access
        LAUNCHER (20) - Execute runs (launchRun, terminateRun)
        EDITOR (30)   - Manage schedules, sensors, assets
        ADMIN (40)    - Full access + user management
    """

    VIEWER = 10
    LAUNCHER = 20
    EDITOR = 30
    ADMIN = 40


# ========================================
# Role Permissions (RBAC)
# ========================================

class RolePermissions:
    """
    Defines GraphQL mutation permissions for each role.

    Permissions are hierarchical:
    - ADMIN can do everything (ADMIN + EDITOR + LAUNCHER)
    - EDITOR can do EDITOR + LAUNCHER
    - LAUNCHER can do only LAUNCHER
    - VIEWER can only read
    """

    # Unrestricted — allowed for all roles (e.g., telemetry)
    UNRESTRICTED_MUTATIONS: Set[str] = frozenset(
        {
            "logTelemetry",
        }
    )

    # LAUNCHER (20) - Execution Operations
    LAUNCHER_MUTATIONS: Set[str] = frozenset(
        {
            "launchRun",
            "launchPipelineExecution",
            "launchRunReexecution",
            "launchPipelineReexecution",
            "terminateRun",
            "terminatePipelineExecution",
            "terminateRuns",
            "deleteRun",
            "deletePipelineRun",
        }
    )

    # EDITOR (30) - Configuration & Management
    EDITOR_MUTATIONS: Set[str] = frozenset(
        {
            # Schedules
            "startSchedule",
            "stopRunningSchedule",
            "resetSchedule",
            "scheduleDryRun",
            # Sensors
            "startSensor",
            "stopSensor",
            "resetSensor",
            "setSensorCursor",
            "sensorDryRun",
            # Assets
            "wipeAssets",
            "reportRunlessAssetEvents",
            "setAutoMaterializePaused",
            # Backfills
            "launchPartitionBackfill",
            "launchBackfill",
            "cancelPartitionBackfill",
            "resumePartitionBackfill",
            "reexecutePartitionBackfill",
            # Partitions
            "addDynamicPartition",
            "deleteDynamicPartitions",
            # Multiple runs
            "launchMultipleRuns",
            # Concurrency
            "setConcurrencyLimit",
            "deleteConcurrencyLimit",
            "freeConcurrencySlotsForRun",
            "freeConcurrencySlots",
        }
    )

    # ADMIN (40) - System Operations
    ADMIN_MUTATIONS: Set[str] = frozenset(
        {
            "reloadRepositoryLocation",
            "reloadWorkspace",
            "shutdownRepositoryLocation",
        }
    )

    # Mutations whose Dagster resolvers check permissions per code location
    # (via has_permission_for_location / _for_run / _for_selector / _for_backfill).
    # With location-scoped RBAC enabled, the middleware lets these through on
    # the user's highest location role and Dagster enforces the location.
    # Every other mutation is gated on the user's global role only.
    # Verified against Dagster 1.13.27 — re-check when adding entries.
    LOCATION_SCOPED_MUTATIONS: Set[str] = frozenset(
        {
            # Runs
            "launchRun",
            "launchPipelineExecution",
            "launchMultipleRuns",
            "launchRunReexecution",
            "launchPipelineReexecution",
            "terminateRun",
            "terminatePipelineExecution",
            "terminateRuns",
            "deleteRun",
            "deletePipelineRun",
            # Schedules
            "startSchedule",
            "stopRunningSchedule",
            "resetSchedule",
            "scheduleDryRun",
            # Sensors
            "startSensor",
            "stopSensor",
            "resetSensor",
            "setSensorCursor",
            "sensorDryRun",
            # Assets
            "wipeAssets",
            "reportRunlessAssetEvents",
            # Backfills
            "launchPartitionBackfill",
            "cancelPartitionBackfill",
            "resumePartitionBackfill",
            "reexecutePartitionBackfill",
            # Partitions
            "addDynamicPartition",
            "deleteDynamicPartitions",
            # Code locations
            "reloadRepositoryLocation",
            "shutdownRepositoryLocation",
        }
    )

    @classmethod
    def get_required_role(
        cls, mutation_name: str, default_role: Optional[Role] = None
    ) -> Optional[Role]:
        """
        Get the minimum required role for a GraphQL mutation.

        Args:
            mutation_name: Name of the GraphQL mutation (e.g. ``launchRun``).
            default_role:  Role for unrecognized mutations. ``None``
                           (legacy) means no restriction. Set to
                           ``Role.ADMIN`` for deny-by-default.

        Returns:
            Required ``Role``, ``default_role`` for unknown mutations,
            or ``None`` if no restriction.

        Example::

            >>> RolePermissions.get_required_role("launchRun")
            Role.LAUNCHER
            >>> RolePermissions.get_required_role("newMutation", Role.ADMIN)
            Role.ADMIN
        """
        if mutation_name == "__UNPARSEABLE_QUERY__":
            # Safety net: if extract_mutation_names is called without is_parseable()
            # check first, the sentinel must fail-closed (require ADMIN).
            if default_role is not None:
                return default_role
            return Role.ADMIN

        if mutation_name in cls.UNRESTRICTED_MUTATIONS:
            return None

        if mutation_name in cls.LAUNCHER_MUTATIONS:
            return Role.LAUNCHER
        elif mutation_name in cls.EDITOR_MUTATIONS:
            return Role.EDITOR
        elif mutation_name in cls.ADMIN_MUTATIONS:
            return Role.ADMIN
        return default_role  # No restriction if no default_role configured

    @classmethod
    def can_execute(cls, user_role: Role, mutation_name: str,
                    default_role: Optional[Role] = None) -> bool:
        """
        Check if a role can execute a mutation (respects role hierarchy).

        Args:
            user_role:     User's role.
            mutation_name: Name of the GraphQL mutation.
            default_role:  Default role for unknown mutations
                           (default: ``None`` — no restriction;
                           pass ``Role.ADMIN`` for deny-by-default).

        Returns:
            ``True`` if the user can execute, ``False`` otherwise.

        Example::

            >>> RolePermissions.can_execute(Role.LAUNCHER, "launchRun")
            True
            >>> RolePermissions.can_execute(Role.VIEWER, "launchRun")
            False
        """
        required_role = cls.get_required_role(mutation_name, default_role)
        if required_role is None:
            return True  # No restriction
        return user_role >= required_role

    @classmethod
    def list_permissions(cls, role: Role) -> Set[str]:
        """
        List all mutations a role can execute.

        Includes inherited permissions from lower roles.

        Args:
            role: Role to check

        Returns:
            Set of mutation names

        Example:
            >>> perms = RolePermissions.list_permissions(Role.EDITOR)
            >>> "launchRun" in perms
            True
            >>> "startSchedule" in perms
            True
        """
        permissions = set()

        if role >= Role.LAUNCHER:
            permissions.update(cls.LAUNCHER_MUTATIONS)
        if role >= Role.EDITOR:
            permissions.update(cls.EDITOR_MUTATIONS)
        if role >= Role.ADMIN:
            permissions.update(cls.ADMIN_MUTATIONS)

        return permissions


# ========================================
# AUTH USER (Universal Representation)
# ========================================


@dataclass
class AuthUser:
    """
    Universal user representation (backend-agnostic).

    All backends (SQLite, LDAP, etc) return this format.

    Attributes:
        username: Unique username
        role: Role level (VIEWER/LAUNCHER/EDITOR/ADMIN), applies everywhere
        email: Email address (optional)
        full_name: Display name (optional)
        location_roles: Extra per-code-location roles, keyed by lowercased
                        location name (optional, see location-scoped RBAC)
        global_access: Whether ``role`` was explicitly granted (e.g. via a
                       plain ``viewers`` group) rather than defaulted. With
                       ``DAGSTER_AUTH_LOCATION_VISIBILITY=granted``, users
                       without it only see their granted locations.
    """

    username: str
    role: Role
    email: str = ""
    full_name: str = ""
    location_roles: Dict[str, Role] = field(default_factory=dict)
    global_access: bool = True

    def can(self, required_role: Role) -> bool:
        """
        Check if user has sufficient permissions.

        Args:
            required_role: Minimum required role

        Returns:
            True if user.role >= required_role

        Example:
            >>> user = AuthUser("john", Role.EDITOR)
            >>> user.can(Role.LAUNCHER)  # True (30 >= 20)
            >>> user.can(Role.ADMIN)     # False (30 < 40)
        """
        return self.role >= required_role

    def role_for_location(self, location_name: str) -> Role:
        """
        Effective role in a code location: the higher of the global role and
        any role granted for that location.

        Args:
            location_name: Dagster code location name (case-insensitive).
        """
        scoped = self.location_roles.get(location_name.lower())
        return max(self.role, scoped) if scoped is not None else self.role

    @property
    def highest_role(self) -> Role:
        """Highest role the user holds anywhere (global or any location)."""
        return max([self.role, *self.location_roles.values()])

    def can_see_location(self, location_name: str) -> bool:
        """
        Whether the location is visible to the user when visibility is
        restricted to granted locations (``DAGSTER_AUTH_LOCATION_VISIBILITY=granted``).

        Args:
            location_name: Dagster code location name (case-insensitive).
        """
        return self.global_access or location_name.lower() in self.location_roles

    @property
    def display_name(self) -> str:
        """Friendly name for UI (fallback to username)."""
        return self.full_name or self.username

    def to_dict(self) -> Dict[str, Any]:
        """
        Convert to dict (for session storage).

        Uses role.value (int) as the canonical representation for consistency
        across all backends (SQL, LDAP, OIDC). from_dict() handles both int
        and string formats for backward compatibility with existing sessions.

        Returns:
            Dict with username, role, email, full_name, plus location_roles
            and global_access only when non-default (to keep cookies small)
        """
        data = {
            "username": self.username,
            "role": self.role.value,
            "email": self.email,
            "full_name": self.full_name,
        }
        if self.location_roles:
            data["location_roles"] = {
                location: role.value for location, role in self.location_roles.items()
            }
        if not self.global_access:
            data["global_access"] = False
        return data

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> "AuthUser":
        """
        Create AuthUser from dict.
        Robust enough to handle both String names and Integer values.
        """
        return cls(
            username=data["username"],
            role=_resolve_role(data["role"]),
            email=data.get("email", ""),
            full_name=data.get("full_name", ""),
            location_roles={
                location: _resolve_role(raw_role)
                for location, raw_role in (data.get("location_roles") or {}).items()
            },
            global_access=data.get("global_access", True),
        )


def _resolve_role(raw_role: Any) -> Role:
    """Resolve a role stored either by name (``"ADMIN"``) or value (``40``)."""
    if isinstance(raw_role, str):
        return Role[raw_role]
    return Role(raw_role)


# ========================================
# ABSTRACT BACKEND
# ========================================


class AuthBackend(ABC):
    """
    Abstract base class for authentication backends.

    All backends (SQLite, LDAP, OAuth, etc.) must inherit from this class
    and implement the abstract methods.
    """

    def __init__(self, config: Dict[str, Any]):
        """
        Initializes backend with configuration.

        Args:
            config: Dict with settings (usually config.__dict__)
        """
        self.config = config

    @abstractmethod
    def authenticate(self, username: str, password: str) -> Optional[AuthUser]:
        """
        Authenticates user with username and password.

        Args:
            username: Username
            password: Password in plain text

        Returns:
            AuthUser if authenticated, None if failed
        """
        pass

    @abstractmethod
    def get_user(self, username: str) -> Optional[AuthUser]:
        """
        Fetches user info without authenticating.

        Used for session validation (already authenticated).

        Args:
            username: Username

        Returns:
            AuthUser or None if user doesn't exist
        """
        pass

    @abstractmethod
    def get_name(self) -> str:
        """
        Returns backend identifier.

        Returns:
            str: Backend name (e.g., 'sqlite', 'ldap', 'dummy')
        """
        pass

    # ========================================
    # OPTIONAL METHODS (for local backends)
    # ========================================

    def add_user(
        self,
        username: str,
        password: str,
        role: Role,
        email: str = "",
        full_name: str = "",
        performed_by: str = "system",
        **kwargs,
    ) -> bool:
        """
        Creates a new user (OPTIONAL - only for local backends).

        Args:
            username: Username
            password: Password in plain text (will be hashed)
            role: Role level (Role.ADMIN, Role.EDITOR, etc)
            email: User's email
            full_name: Display name
            performed_by: Who created the user (for auditing)

        Returns:
            bool: True if created, False if failed

        Raises:
            NotImplementedError: If backend doesn't support it (LDAP, OAuth)
        """
        raise NotImplementedError(f"{self.get_name()} backend does not support user creation")

    def delete_user(self, username: str, performed_by: str = "system") -> bool:
        """
        Removes a user (OPTIONAL - only for local backends).

        Args:
            username: Username
            performed_by: Who deleted the user (for auditing)

        Returns:
            bool: True if deleted, False if user doesn't exist

        Raises:
            NotImplementedError: If backend doesn't support it
        """
        raise NotImplementedError(f"{self.get_name()} backend does not support user deletion")

    def change_password(
        self, username: str, new_password: str, performed_by: str = "system"
    ) -> bool:
        """
        Changes a user's password (OPTIONAL - only for local backends).

        Args:
            username: Username
            new_password: New password in plain text (will be hashed)
            performed_by: Who changed it (for auditing)

        Returns:
            bool: True if changed, False if user doesn't exist

        Raises:
            NotImplementedError: If backend doesn't support it
        """
        raise NotImplementedError(f"{self.get_name()} backend does not support password changes")

    def list_users(self) -> List[AuthUser]:
        """
        Lists all users (OPTIONAL - only for local backends).

        Returns:
            List of AuthUser instances

        Raises:
            NotImplementedError: If backend doesn't support it
        """
        raise NotImplementedError(f"{self.get_name()} backend does not support user listing")

    def change_role(self, username: str, new_role: Role, performed_by: str = "system") -> bool:
        """
        Changes a user's role (OPTIONAL).

        Args:
            username: Username
            new_role: New role level
            performed_by: Who changed it (for auditing)

        Returns:
            bool: True if updated

        Raises:
            NotImplementedError: If backend doesn't support it
        """
        raise NotImplementedError(f"{self.get_name()} backend does not support role changes")
