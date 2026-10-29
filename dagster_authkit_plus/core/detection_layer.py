"""
API Compatibility Detection Layer

Checks if the internal Dagster API has changed in ways that would break
monkey-patching. Critical to prevent silent failures.
"""

import logging
import sys
from typing import Optional, Tuple

logger = logging.getLogger(__name__)

# AuthKit version
__version__ = "1.2.0"  # x-release-please-version

# Compatible Dagster versions (January 2026)
# CRITICAL: Based on Dagster 1.12.12 (latest stable)
COMPATIBLE_DAGSTER_VERSIONS = [
    "1.12.x (primary - fully tested)",
    "1.11.x (supported - tested)",
    "1.10.x (supported - tested)",
]
PRIMARY_DAGSTER_VERSION = "1.12"  # Current recommended version

# Note: Versions 1.9.x and earlier may work but have not been tested
# for this release. Report issues if using older versions.


def verify_dagster_api_compatibility() -> Tuple[bool, Optional[str]]:
    """
    Checks if the internal Dagster API is compatible.

    Returns:
        Tuple[bool, Optional[str]]: (is_compatible, error_message)
    """
    try:
        import dagster
        import dagster_webserver.webserver as webserver_module

    except ImportError as e:
        return False, f"Cannot import Dagster modules: {e}"

    # 1. Check critical webserver module exports
    required_exports = [
        "DagsterWebserver",
    ]

    missing_exports = []
    for export in required_exports:
        if not hasattr(webserver_module, export):
            missing_exports.append(export)

    if missing_exports:
        return False, (
            f"Dagster API changed! Missing exports: {', '.join(missing_exports)}. "
            f"AuthKit {__version__} is compatible with Dagster {', '.join(COMPATIBLE_DAGSTER_VERSIONS)}"
        )

    # 2. Check critical methods of DagsterWebserver class
    webserver_class = getattr(webserver_module, "DagsterWebserver")
    required_methods = [
        "build_middleware",
        "build_routes",
    ]

    missing_methods = []
    for method in required_methods:
        if not hasattr(webserver_class, method):
            missing_methods.append(method)

    if missing_methods:
        return False, (
            f"DagsterWebserver API changed! Missing methods: {', '.join(missing_methods)}. "
            f"AuthKit {__version__} is compatible with Dagster {', '.join(COMPATIBLE_DAGSTER_VERSIONS)}"
        )

    # 2b. Location-scoped RBAC hooks into Dagster's per-request permission
    # context. If those internals move, refuse to start rather than silently
    # running without location scoping.
    from dagster_authkit_plus.utils.config import config

    if config.DAGSTER_AUTH_LOCATION_GROUP_PATTERN:
        error = _check_location_rbac_api(webserver_class)
        if not error and config.DAGSTER_AUTH_LOCATION_VISIBILITY == "granted":
            error = _check_location_visibility_api()
        if error:
            return False, (
                f"{error} DAGSTER_AUTH_LOCATION_GROUP_PATTERN is not supported on Dagster "
                f"{getattr(dagster, '__version__', 'unknown')}; unset it or change Dagster version."
            )

    # 3. Check Dagster version
    dagster_version = getattr(dagster, "__version__", "unknown")
    logger.info(f"Dagster version detected: {dagster_version}")

    # Parse major.minor version
    try:
        version_parts = dagster_version.split(".")
        major = int(version_parts[0])
        minor = int(version_parts[1])

        # Primary support: 1.12.x (current stable)
        if major == 1 and minor == 12:
            logger.info(f"✅ Dagster {dagster_version} is FULLY SUPPORTED (primary target)")

        # Tested support: 1.11.x, 1.10.x
        elif major == 1 and minor in [11, 10]:
            logger.info(f"✅ Dagster {dagster_version} is SUPPORTED (tested)")

        # Future versions: 1.13+
        elif major == 1 and minor >= 13:
            logger.warning(
                f"⚠️  Dagster {dagster_version} is NEWER than tested versions. "
                f"May work but not fully tested yet. "
                f"Please report any issues!"
            )

        # Older versions: 1.9.x and below
        elif major == 1 and minor <= 9:
            logger.warning(
                f"⚠️  Dagster {dagster_version} is OLDER than tested versions. "
                f"Recommended: 1.12.x (primary), 1.11.x or 1.10.x (tested). "
                f"AuthKit may not work correctly with older versions."
            )

        # Very old or unknown
        else:
            logger.warning(
                f"⚠️  Dagster {dagster_version} is OUTSIDE tested range. "
                f"Recommended: 1.12.x, 1.11.x, or 1.10.x. "
                f"AuthKit may not work correctly."
            )

    except (ValueError, IndexError):
        logger.warning(f"Could not parse Dagster version: {dagster_version}")

    # 4. Verify expected middleware structure
    try:
        from starlette.middleware import Middleware

        # Verify the Middleware constructor accepts the expected signature
        mw = Middleware(lambda app: app)
        if mw.cls is None:
            return False, "Starlette Middleware constructor returned unexpected type"
    except Exception as e:
        return False, f"Starlette middleware API incompatible: {e}"

    return True, None


def _check_location_rbac_api(webserver_class) -> Optional[str]:
    """Return an error message if the Dagster internals used by location RBAC are missing."""
    from dagster_authkit_plus.core.location_rbac import (
        PERMISSION_ROLES,
        request_context_method_name,
    )

    if request_context_method_name(webserver_class) is None:
        return "DagsterWebserver._make_request_context not found."

    try:
        from dagster._core.workspace.context import WorkspaceRequestContext
        from dagster._core.workspace.permissions import Permissions
    except ImportError as e:
        return f"Dagster permission API not found: {e}."

    for attr in ("permissions", "permissions_for_location", "has_permission_for_location"):
        if not hasattr(WorkspaceRequestContext, attr):
            return f"WorkspaceRequestContext.{attr} not found."

    unmapped = sorted(p.value for p in Permissions if p.value not in PERMISSION_ROLES)
    if unmapped:
        # Not fatal: unmapped permissions require DAGSTER_AUTH_UNKNOWN_MUTATION_ROLE.
        logger.warning(
            f"Dagster permissions without an AuthKit role mapping (will require "
            f"DAGSTER_AUTH_UNKNOWN_MUTATION_ROLE): {', '.join(unmapped)}"
        )
    return None


def _check_location_visibility_api() -> Optional[str]:
    """Return an error message if the Dagster internals used to hide locations are missing."""
    from dagster_authkit_plus.core.location_visibility import FILTERED_INSTANCE_METHODS

    try:
        from dagster import DagsterInstance
        from dagster._core.workspace.workspace import CurrentWorkspace  # noqa: F401
        from dagster._record import copy  # noqa: F401
    except ImportError as e:
        return f"Dagster workspace/instance API not found: {e}."

    missing = [m for m in FILTERED_INSTANCE_METHODS if not hasattr(DagsterInstance, m)]
    if missing:
        return f"DagsterInstance methods not found: {', '.join(missing)}."
    return None


def get_compatibility_report() -> str:
    """
    Generates detailed compatibility report.

    Returns:
        str: Formatted report
    """
    try:
        import dagster
        import dagster_webserver
    except ImportError:
        return "❌ Dagster not installed"

    dagster_version = getattr(dagster, "__version__", "unknown")
    webserver_version = getattr(dagster_webserver, "__version__", "unknown")

    is_compatible, error = verify_dagster_api_compatibility()

    report = f"""
╔════════════════════════════════════════════════════════════╗
║           DAGSTER AUTHKIT - COMPATIBILITY REPORT           ║
╚════════════════════════════════════════════════════════════╝

AuthKit Version:     {__version__}
Compatible Dagster:  {', '.join(COMPATIBLE_DAGSTER_VERSIONS)}

Detected Versions:
  - Dagster:         {dagster_version}
  - Webserver:       {webserver_version}
  - Python:          {sys.version.split()[0]}

Compatibility Check: {'✅ COMPATIBLE' if is_compatible else '❌ INCOMPATIBLE'}
"""

    if not is_compatible:
        report += f"\nError Details:\n  {error}\n"
        report += "\nAction Required:\n"
        report += "  1. Update Dagster to compatible version, OR\n"
        report += "  2. Update AuthKit to match your Dagster version, OR\n"
        report += "  3. Report issue: https://github.com/electrophys/dagster-authkit-plus/issues\n"
    else:
        report += "\n✅ All API checks passed. AuthKit should work correctly.\n"

    return report


def print_compatibility_warning():
    """Prints compatibility warning if needed."""
    is_compatible, error = verify_dagster_api_compatibility()

    if not is_compatible:
        print("\n" + "=" * 60)
        print("⚠️  DAGSTER COMPATIBILITY WARNING")
        print("=" * 60)
        print(f"\n{error}\n")
        print("AuthKit may not function correctly!")
        print("See compatibility report: dagster-authkit-plus --version\n")
        print("=" * 60 + "\n")


def check_and_exit_if_incompatible():
    """
    Checks compatibility and exits with error if incompatible.
    Useful for CI/CD.
    """
    is_compatible, error = verify_dagster_api_compatibility()

    if not is_compatible:
        logger.error("Dagster API compatibility check FAILED")
        logger.error(error)
        print(get_compatibility_report())
        sys.exit(1)

    logger.info("Dagster API compatibility check PASSED")
