"""
Proxy Authentication Backend (Authelia Forward Auth)

Reads authentication headers set by upstream reverse proxy (Authelia).
Does NOT handle sessions, passwords, or rate limiting - that's Authelia's job.

Expected Headers (Authelia defaults):
- Remote-User: username
- Remote-Groups: comma-separated groups (e.g., "cn=admins,ou=groups,dc=company,dc=com")
- Remote-Email: user email
- Remote-Name: full name

Group → Role Mapping:
Configure via DAGSTER_AUTH_PROXY_GROUP_PATTERN:
  cn={role},ou=groups,dc=company,dc=com

Examples:
  - cn=admins,ou=groups,dc=company,dc=com → Role.ADMIN
  - cn=editors,ou=groups,dc=company,dc=com → Role.EDITOR
  - cn=launchers,ou=groups,dc=company,dc=com → Role.LAUNCHER
  - cn=viewers,ou=groups,dc=company,dc=com → Role.VIEWER

If DAGSTER_AUTH_PROXY_GROUP_PATTERN is not set, falls back to simple matching:
  - "admins" → Role.ADMIN
  - "editors" → Role.EDITOR
  - etc.

Location-scoped roles (optional):
Configure via DAGSTER_AUTH_LOCATION_GROUP_PATTERN, e.g. dagster:{location}:{role}.
  - dagster:team_a:launchers → LAUNCHER in code location team_a
These add to the global role above; see auth/location_roles.py.
"""

import logging
from typing import Any, Dict, List, Optional

from dagster_authkit.auth.backends.base import AuthBackend, AuthUser, Role
from dagster_authkit.auth.location_roles import (
    compile_location_group_pattern,
    parse_location_roles,
)

logger = logging.getLogger(__name__)


class ProxyAuthBackend(AuthBackend):
    """
    Proxy-based authentication for Authelia/Traefik forward auth.

    ALL configuration comes from dagster_authkit.utils.config.
    No config parsing here - just uses what's already loaded.
    """

    def __init__(self, config: Dict[str, Any]):
        """
        Initialise proxy backend with header name mappings and group-to-role pattern.

        Args:
            config: Dict from ``AuthConfig`` containing proxy header names,
                    group pattern, and trusted IP settings.
        """
        super().__init__(config)

        # Header names (from centralized config)
        self.user_header = config["DAGSTER_AUTH_PROXY_USER_HEADER"]
        self.groups_header = config["DAGSTER_AUTH_PROXY_GROUPS_HEADER"]
        self.email_header = config["DAGSTER_AUTH_PROXY_EMAIL_HEADER"]
        self.name_header = config["DAGSTER_AUTH_PROXY_NAME_HEADER"]

        # Group → Role mapping pattern
        group_pattern = config.get("DAGSTER_AUTH_PROXY_GROUP_PATTERN")
        if group_pattern:
            # Pattern-based mapping (LDAP DNs)
            # Example: "cn={role},ou=groups,dc=company,dc=com"
            self.group_mappings = {
                Role.ADMIN: group_pattern.replace("{role}", "admins"),
                Role.EDITOR: group_pattern.replace("{role}", "editors"),
                Role.LAUNCHER: group_pattern.replace("{role}", "launchers"),
                Role.VIEWER: group_pattern.replace("{role}", "viewers"),
            }
            logger.info(f"✅ Using LDAP DN pattern: {group_pattern}")
        else:
            # Simple group name matching (case-insensitive)
            # Works with simple Authelia group names
            self.group_mappings = {
                Role.ADMIN: "admins",
                Role.EDITOR: "editors",
                Role.LAUNCHER: "launchers",
                Role.VIEWER: "viewers",
            }
            logger.info("✅ Using simple group matching (admins, editors, launchers, viewers)")

        # Group → per-location role mapping (optional)
        location_pattern = config.get("DAGSTER_AUTH_LOCATION_GROUP_PATTERN")
        self.location_pattern = (
            compile_location_group_pattern(location_pattern) if location_pattern else None
        )
        if location_pattern:
            logger.info(f"✅ Using location group pattern: {location_pattern}")

        logger.info(
            f"ProxyAuthBackend initialized\n"
            f"  User header: {self.user_header}\n"
            f"  Groups header: {self.groups_header}\n"
            f"  Email header: {self.email_header}\n"
            f"  Name header: {self.name_header}\n"
            f"  Group mappings: {self.group_mappings}"
        )

    def _parse_groups_header(self, groups_raw: str) -> List[str]:
        """
        Parse groups header using multiple fallback strategies.

        Parsing strategies (in order):
        1. JSON array parsing
        2. LDAP DN detection (preserves internal commas)
        3. Multi-delimiter split (comma, semicolon, pipe)
        4. Whitespace split (fallback)

        Args:
            groups_raw: Raw groups header value from upstream proxy

        Returns:
            List of normalized group names (lowercased, deduplicated)

        Examples:
            '["admin","editor"]' -> ['admin', 'editor']
            'admin,editor,viewer' -> ['admin', 'editor', 'viewer']
            'cn=admins,ou=groups;cn=editors,ou=groups' -> ['cn=admins,ou=groups', 'cn=editors,ou=groups']
        """
        import json

        if not groups_raw:
            return []

        groups_raw = groups_raw.strip()
        groups = []

        # Strategy 1: Attempt JSON array parsing
        if groups_raw.startswith("[") and groups_raw.endswith("]"):
            try:
                parsed = json.loads(groups_raw)
                if isinstance(parsed, list):
                    groups = [str(g).strip() for g in parsed if g]
                    logger.debug(f"   Parsed as JSON array: {len(groups)} groups")
            except (json.JSONDecodeError, ValueError) as e:
                logger.debug(f"   JSON parse failed: {e}")

        # Strategy 2: LDAP DN detection
        # Check for LDAP-specific patterns (equals sign with ou= or dc= components)
        if (
            not groups
            and ("=" in groups_raw)
            and ("ou=" in groups_raw.lower() or "dc=" in groups_raw.lower())
        ):
            # Split LDAP DNs by common delimiters (semicolon or pipe)
            # Avoid splitting on commas as they are part of the DN structure
            for delimiter in [";", "|"]:
                if delimiter in groups_raw:
                    groups = [g.strip() for g in groups_raw.split(delimiter) if g.strip()]
                    logger.debug(
                        f"   Parsed as LDAP DNs (delimiter '{delimiter}'): {len(groups)} groups"
                    )
                    break

            # If no delimiter found, treat as single DN
            if not groups:
                groups = [groups_raw]
                logger.debug("   Parsed as single LDAP DN")

        # Strategy 3: Multi-delimiter split
        # Try common delimiters in priority order
        if not groups:
            for delimiter in [",", ";", "|"]:
                if delimiter in groups_raw:
                    candidate = [g.strip() for g in groups_raw.split(delimiter) if g.strip()]
                    # Validation: ensure split produced meaningful results
                    if len(candidate) > 1 or (len(candidate) == 1 and candidate[0]):
                        groups = candidate
                        logger.debug(
                            f"   Parsed with delimiter '{delimiter}': {len(groups)} groups"
                        )
                        break

        # Strategy 4: Whitespace split (fallback)
        if not groups:
            groups = groups_raw.split()
            logger.debug(f"   Parsed with whitespace split: {len(groups)} groups")

        # Normalize and deduplicate
        normalized = []
        seen = set()

        for g in groups:
            g_lower = g.strip().lower()
            if g_lower and g_lower not in seen:
                seen.add(g_lower)
                normalized.append(g_lower)

        logger.debug(f"   Final normalized groups: {normalized}")
        return normalized

    def get_name(self) -> str:
        return "proxy"

    # ========================================
    # Password Auth NOT Supported
    # ========================================

    def authenticate(self, username: str, password: str) -> Optional[AuthUser]:
        """
        NOT USED in proxy mode.
        Authentication happens at Authelia, not here.

        Raises:
            NotImplementedError: Always (passwords handled by Authelia)
        """
        raise NotImplementedError(
            "ProxyAuthBackend does not support password authentication.\n"
            "Authentication is handled by upstream proxy (Authelia).\n"
            "If you're seeing this error, check your AUTH_BACKEND setting."
        )

    def get_user(self, username: str) -> Optional[AuthUser]:
        """
        NOT USED in proxy mode.
        User data comes from HTTP headers, not database lookups.

        Raises:
            NotImplementedError: Always (user data from headers)
        """
        raise NotImplementedError(
            "ProxyAuthBackend does not support get_user().\n"
            "User data is extracted from HTTP headers by middleware.\n"
            "If you're seeing this error, check your middleware implementation."
        )

    # ========================================
    # PROXY-SPECIFIC: Header Parsing
    # ========================================

    def get_user_from_headers(self, headers: Dict[str, str]) -> Optional[AuthUser]:
        """
        Extracts user from HTTP headers set by Authelia.

        This is the MAIN method used by the middleware in proxy mode.

        Args:
            headers: HTTP headers dict (case-insensitive)

        Returns:
            AuthUser if headers valid, None if missing required headers

        Example:
            headers = {
                "Remote-User": "john",
                "Remote-Groups": "cn=admins,ou=groups,dc=company,dc=com",
                "Remote-Email": "john@company.com",
                "Remote-Name": "John Doe"
            }
            user = backend.get_user_from_headers(headers)
            # Returns: AuthUser(username="john", role=Role.ADMIN, ...)
        """
        # Normalize headers to lowercase for case-insensitive lookup
        headers_lower = {k.lower(): v for k, v in headers.items()}

        # Extract username (REQUIRED)
        username = headers_lower.get(self.user_header.lower())
        if not username:
            logger.warning(
                f"❌ Proxy auth: Missing required header '{self.user_header}'\n"
                f"   Available headers: {list(headers.keys())}"
            )
            return None

        # Extract groups (for role mapping)
        groups_raw = headers_lower.get(self.groups_header.lower(), "")
        groups = self._parse_groups_header(groups_raw)

        # Determine per-location roles, then the global role from groups.
        # Users with only location groups legitimately get a VIEWER global
        # role, so don't warn about the default in that case.
        location_roles = (
            parse_location_roles(groups, self.location_pattern) if self.location_pattern else {}
        )
        matched_role = self._match_global_role(groups)
        role = self._determine_role_from_groups(groups, warn_on_default=not location_roles)

        # Extract optional metadata
        email = headers_lower.get(self.email_header.lower(), "")
        full_name = headers_lower.get(self.name_header.lower(), "") or username.capitalize()

        logger.info(
            f"✅ Proxy auth: User '{username}' authenticated\n"
            f"   Groups: {groups}\n"
            f"   Role: {role.name}\n"
            f"   Location roles: {({loc: r.name for loc, r in location_roles.items()}) or 'N/A'}\n"
            f"   Email: {email or 'N/A'}\n"
            f"   Full name: {full_name}"
        )

        return AuthUser(
            username=username,
            role=role,
            email=email,
            full_name=full_name,
            location_roles=location_roles,
            # A defaulted VIEWER role doesn't grant visibility of every location
            global_access=matched_role is not None,
        )

    def _match_global_role(self, groups: List[str]) -> Optional[Role]:
        """Highest role whose global group the user is in, or None if no group matches."""
        for role in [Role.ADMIN, Role.EDITOR, Role.LAUNCHER, Role.VIEWER]:
            target_group = self.group_mappings.get(role, "").lower()
            if target_group and target_group in groups:
                return role
        return None

    def _determine_role_from_groups(self, groups: List[str], warn_on_default: bool = True) -> Role:
        """
        Maps LDAP groups to AuthKit roles.

        Priority: ADMIN > EDITOR > LAUNCHER > VIEWER

        Args:
            groups: List of group DNs or names (already lowercased)

        Returns:
            Highest matching role, defaults to VIEWER if no match

        Example:
            groups = ["cn=admins,ou=groups,dc=company,dc=com", "cn=developers,ou=groups"]
            # Returns: Role.ADMIN (highest priority match)
        """
        role = self._match_global_role(groups)
        if role is not None:
            logger.debug(f"   Matched '{self.group_mappings[role]}' → {role.name}")
            return role

        if not warn_on_default:
            return Role.VIEWER

        logger.warning(
            f"⚠️  No group match found for: {groups}\n"
            f"   Expected one of: {list(self.group_mappings.values())}\n"
            f"   Defaulting to VIEWER role"
        )
        return Role.VIEWER

    # ========================================
    # User Management NOT Supported
    # (Users managed in Authelia/LDAP)
    # ========================================

    def add_user(self, *args, **kwargs) -> bool:
        """Not supported - users managed in Authelia/LDAP."""
        raise NotImplementedError(
            "ProxyAuthBackend does not support user management.\n"
            "Users are managed in your Authelia configuration or LDAP server."
        )

    def delete_user(self, *args, **kwargs) -> bool:
        """Not supported - users managed in Authelia/LDAP."""
        raise NotImplementedError(
            "ProxyAuthBackend does not support user management.\n"
            "Users are managed in your Authelia configuration or LDAP server."
        )

    def change_password(self, *args, **kwargs) -> bool:
        """Not supported - passwords managed in Authelia/LDAP."""
        raise NotImplementedError(
            "ProxyAuthBackend does not support password changes.\n"
            "Passwords are managed in your Authelia configuration or LDAP server."
        )

    def list_users(self) -> List[AuthUser]:
        """Not supported - query LDAP directly."""
        raise NotImplementedError(
            "ProxyAuthBackend does not support user listing.\n"
            "Query your LDAP server or Authelia user database directly."
        )

    def change_role(self, *args, **kwargs) -> bool:
        """Not supported - roles managed via LDAP groups."""
        raise NotImplementedError(
            "ProxyAuthBackend does not support role changes.\n"
            "Roles are determined by LDAP group membership. "
            "Manage group assignments in your LDAP server."
        )
