"""
Location-scoped role parsing.

Turns identity-provider group names into per-code-location roles, using a
pattern configured via ``DAGSTER_AUTH_LOCATION_GROUP_PATTERN``.

The pattern must contain exactly one ``{location}`` and one ``{role}``
placeholder. ``{role}`` accepts the singular or plural role name
(``admin``/``admins``, ``editor``/``editors``, ``launcher``/``launchers``,
``viewer``/``viewers``).

Examples::

    Pattern                         Group                          Result
    dagster:{location}:{role}       dagster:team_a:launchers       team_a → LAUNCHER
    /dagster/{location}/{role}      /dagster/team_b/editors        team_b → EDITOR

Matching is case-insensitive and location names are stored lowercased,
because the proxy backend lowercases every group it receives.
"""

import re
from typing import Dict, Iterable, Pattern

from dagster_authkit.auth.backends.base import Role

LOCATION_PLACEHOLDER = "{location}"
ROLE_PLACEHOLDER = "{role}"

_ROLE_WORDS: Dict[str, Role] = {
    "admin": Role.ADMIN,
    "admins": Role.ADMIN,
    "editor": Role.EDITOR,
    "editors": Role.EDITOR,
    "launcher": Role.LAUNCHER,
    "launchers": Role.LAUNCHER,
    "viewer": Role.VIEWER,
    "viewers": Role.VIEWER,
}


def compile_location_group_pattern(pattern: str) -> Pattern[str]:
    """
    Compile a location group pattern into a regex.

    Args:
        pattern: Pattern with one ``{location}`` and one ``{role}`` placeholder.

    Returns:
        Compiled, case-insensitive regex with ``location`` and ``role`` groups.

    Raises:
        ValueError: If either placeholder is missing or repeated.
    """
    for placeholder in (LOCATION_PLACEHOLDER, ROLE_PLACEHOLDER):
        if pattern.count(placeholder) != 1:
            raise ValueError(
                f"Invalid DAGSTER_AUTH_LOCATION_GROUP_PATTERN: {pattern!r}. "
                f"It must contain exactly one {LOCATION_PLACEHOLDER} and one "
                f"{ROLE_PLACEHOLDER} placeholder, e.g. 'dagster:{{location}}:{{role}}'."
            )

    regex = re.escape(pattern)
    regex = regex.replace(re.escape(LOCATION_PLACEHOLDER), r"(?P<location>.+?)")
    regex = regex.replace(re.escape(ROLE_PLACEHOLDER), r"(?P<role>[a-z]+)")
    return re.compile(regex, re.IGNORECASE)


def parse_location_roles(groups: Iterable[str], pattern: Pattern[str]) -> Dict[str, Role]:
    """
    Extract per-location roles from a list of group names.

    Groups that don't match the pattern, or whose role word is unknown, are
    ignored. When a user is in several groups for the same location, the
    highest role wins.

    Args:
        groups:  Group names from the identity provider.
        pattern: Regex from :func:`compile_location_group_pattern`.

    Returns:
        Mapping of lowercased location name to role.
    """
    location_roles: Dict[str, Role] = {}
    for group in groups:
        match = pattern.fullmatch(group.strip())
        if not match:
            continue
        role = _ROLE_WORDS.get(match.group("role").lower())
        if role is None:
            continue
        location = match.group("location").lower()
        if role > location_roles.get(location, 0):
            location_roles[location] = role
    return location_roles
