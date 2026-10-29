# 🛡️ Dagster AuthKit Plus

[![Python Version](https://img.shields.io/badge/python-3.10%20|%203.11%20|%203.12%20|%203.13%20|%203.14-blue.svg?logo=python&logoColor=white)](https://www.python.org/downloads/)
[![Build Status](https://github.com/electrophys/dagster-authkit-plus/workflows/CI/badge.svg)](https://github.com/electrophys/dagster-authkit-plus/actions)
[![License](https://img.shields.io/badge/license-Apache%202.0-green.svg?logo=open-source-initiative&logoColor=white)](https://opensource.org/licenses/Apache-2.0)
[![PyPI Version](https://img.shields.io/pypi/v/dagster-authkit-plus?color=blue&logo=pypi&logoColor=white)](https://pypi.org/project/dagster-authkit-plus/)

**Community authentication wrapper for self-hosted Dagster OSS.**

*Authentication, RBAC, and Audit logs for Dagster without touching internal code.*

> **dagster-authkit-plus** is a fork of [dagster-authkit](https://github.com/maltzsama/dagster-authkit) that adds
> [roles per code location](#roles-per-code-location-proxy-mode) and the option to
> [hide other teams' code locations](#hiding-other-teams-code-locations). Configuration is unchanged
> (the same `DAGSTER_AUTH_*` environment variables); the package is `dagster-authkit-plus`, the import
> package is `dagster_authkit_plus` and the command is `dagster-authkit-plus`. Uninstall `dagster-authkit`
> before installing it.

---

## 🎯 What is this?

Dagster OSS has no auth. If you run it in a VPC or locally, anyone with the URL has full admin access.

**AuthKit solves this by wrapping the `dagster-webserver` command to add:**

* ✅ **Login Interface:** Simple username/password flow.
* ✅ **RBAC (4 Levels):** Granular control over who can do what.
* ✅ **Audit Logs:** JSON logs for monitoring who is doing what.
* ✅ **Multi-Backend:** Works with SQLite, Postgres, MySQL (via Peewee ORM) and Redis.

**No code changes required.** You don't touch your `repository.py` or `dagster.yaml`.

---

## ✨ What's New in the Plus fork

- **Roles per code location** from identity-provider groups, enforced through Dagster's own permission checks (the UI disables buttons where the user can't act).
- **Hiding code locations** a user holds no role in: workspace, runs, logs, backfills and asset history.
- Tested on Dagster 1.10–1.13.

## ✨ What's New in dagster-authkit v1.0.0

### ☸️ Helm Chart
- **Production-ready Kubernetes chart** under `helm/dagster-authkit/`, synced with the application's config (env vars, secrets, image tag) and versioned automatically on each release.

### 🔄 Automated CHANGELOG
- **`CHANGELOG.md` is now auto-generated** on each release, alongside the version bump.
- **RBAC deny-by-default** — unknown GraphQL mutations require `ADMIN` by default.

### 🔐 Hardening & Fixes
- **Rebuild of the fix backlog from v0.4.2** across CSRF (per-client double-submit), fail-closed GraphQL batches, token-gated `/auth/metrics`, Redis 6-compatible atomic rate limiting, LDAP connection cleanup, session revoked-token caps, and detection-layer Starlette `Middleware.cls` compatibility.
- **Test coverage boosted** 44% → 52% (+98 tests: detection layer, rate limiter, metrics gate, CookieBackend, CLI, LDAP, middleware).

---

## ⚠️ Upgrading from v0.3.x

1. **Set `DAGSTER_AUTH_SECRET_KEY`** in your environment. Generate one with:
   ```bash
   python -c 'import secrets; print(secrets.token_urlsafe(32))'
   ```
2. **If using proxy mode**, set `DAGSTER_AUTH_PROXY_TRUSTED_IPS` to your proxy's IP address.
3. **Database migration** happens automatically on first boot — no manual steps needed for SQLite/Postgres/MySQL. A `session_version` column is added to the `users` table.
4. **Role serialization** changed from string (`"ADMIN"`) to int (`40`) in session cookies. Existing sessions continue to work (backward-compatible `from_dict`).

---

## 📂 Ready-to-Run Examples

We provide ready-to-use stacks for different scenarios in the `examples/` directory:

```bash
examples
├── authelia              # NEW! Authelia + Caddy + LDAP SSO (Docker)
│   ├── Makefile
│   ├── docker-compose.yml
│   ├── Caddyfile
│   └── authelia/
├── kubernetes            # NEW! Minikube deployment
│   ├── Makefile
│   └── k8s/
├── ldap                  # Active Directory integration (**Experimental**)
│   ├── Makefile
│   ├── docker-compose.yml
│   └── ldap-bootstrap.ldif
├── postgresql_redis      # Recommended production setup
│   ├── Makefile
│   └── docker-compose.yml
└── quickstart-sqlite     # Simple local testing
    ├── Makefile
    └── docker-compose.yml
```

### How to run

Pick a scenario, go into the folder, and check the `Makefile`.

**1. Authelia SSO (Docker)**
Complete SSO with Authelia, Caddy, and OpenLDAP:
```bash
cd examples/authelia
make up
# Access: https://auth.company.com (admin/password123)
# Then:   https://dagster.company.com
```

**2. Kubernetes (Minikube)**
Same stack running on Kubernetes:
```bash
cd examples/kubernetes
make build  # Build the Docker image inside Minikube
make up     # Deploy everything
# In another terminal: make connect (runs minikube tunnel)
# Add to /etc/hosts: $(minikube ip) auth.company.com dagster.company.com
```

**3. Standard Setup (Postgres + Redis)**
```bash
cd examples/postgresql_redis
make up
```

**4. Local Quickstart (SQLite)**
```bash
cd examples/quickstart-sqlite
make up
```

**5. LDAP/AD Testing** ⚠️ **EXPERIMENTAL**
```bash
cd examples/ldap
make up
```

---

## 🚀 Manual Installation (Python)

If you aren't using Docker, you can install via pip.

```bash
# For local testing (SQLite)
pip install dagster-authkit-plus[sqlite]

# For server usage (Postgres + Redis recommended)
pip install dagster-authkit-plus[postgresql,redis]

# For LDAP/Active Directory integration (**Experimental**)
pip install dagster-authkit-plus[ldap]

```

**Usage:**

```bash
# Initialize the database and create the first admin
dagster-authkit-plus init-db --with-admin

# Run Dagster (replaces the standard 'dagster-webserver' command)
dagster-authkit-plus -f your_pipeline.py -h 0.0.0.0 -p 3000

# For proxy mode (Authelia/OAuth2 Proxy)
export DAGSTER_AUTH_BACKEND=proxy
export DAGSTER_AUTH_PROXY_LOGIN_URL=https://auth.yourcompany.com
dagster-authkit-plus -f your_pipeline.py -h 0.0.0.0 -p 3000
```

### ☸️ Helm (Kubernetes)

This fork publishes neither the chart nor a container image: build the image from the `Dockerfile`, push it to your registry and install the chart from a checkout. See the [chart README](./helm/dagster-authkit/README.md).

---

## 🔐 Roles (RBAC)

We provide **4 levels** of access. Permissions are enforced via GraphQL query analysis.

| Role | Description |
|------|-------------|
| **Admin** | Full access. Can manage users, settings, and all pipelines. |
| **Editor** | Can modify assets and codebase (if allowed) and manage runs. |
| **Launcher** | Can launch runs and re-execute jobs, but cannot modify code/assets. |
| **Viewer** | Read-only. Can view runs and assets. GraphQL mutations are blocked. |

**How it works:** AuthKit analyzes GraphQL queries using the official GraphQL parser to accurately identify mutations and block unauthorized actions.

### Roles per code location (proxy mode)

Grant roles in specific code locations through group names. The location and role are read from each group using a pattern:

```bash
export DAGSTER_AUTH_LOCATION_GROUP_PATTERN='dagster:{location}:{role}'
# Keycloak with "Full group path" enabled:
# export DAGSTER_AUTH_LOCATION_GROUP_PATTERN='/dagster/{location}/{role}'
```

| User's groups | Result |
|---------------|--------|
| `dagster:team_a:launchers` | LAUNCHER in `team_a`, VIEWER everywhere else |
| `dagster:team_a:editors`, `dagster:team_b:launchers` | EDITOR in `team_a`, LAUNCHER in `team_b` |
| `launchers`, `dagster:team_a:editors` | LAUNCHER everywhere, EDITOR in `team_a` |

- `{role}` accepts `admin(s)`, `editor(s)`, `launcher(s)` or `viewer(s)`. Matching is case-insensitive, including location names.
- A location role can only *add* to the global role (the plain `admins`/`editors`/… groups), never lower it.
- AuthKit hooks into Dagster's own per-location permission checks, so they cover launches, re-execution, terminate/delete by run ID, schedules, sensors, backfills, dynamic partitions, asset wipes and code location reloads. The Dagster UI disables buttons for locations the user can't act on.
- Workspace-wide actions (reload workspace, concurrency limits, auto-materialize toggle) use the global role only.
- Only the proxy backend reads location groups today. AuthKit refuses to start if the installed Dagster version lacks the permission API it hooks into (tested on Dagster 1.10–1.13).

#### Hiding other teams' code locations

By default every signed-in user can *see* all code locations. To hide the ones a user has no role in:

```bash
export DAGSTER_AUTH_LOCATION_VISIBILITY=granted
```

A user in any plain global group (`admins`, `editors`, `launchers`, `viewers`) still sees everything. Anyone else sees only the locations they have a group for, so use `dagster:<location>:viewers` to give read-only access to a single location. A user with no matching groups sees an empty workspace.

What gets hidden:

| Area | Behaviour |
|------|-----------|
| Code locations, jobs, schedules, sensors, asset definitions, lineage | Not listed (removed from the user's workspace) |
| Runs list, runs feed, run counts | Only runs from visible locations (filtered in the database query) |
| Run page, run event logs, captured stdout/stderr | "Run not found" / empty for runs in hidden locations |
| Backfills | Only job backfills in visible locations, and asset backfills whose assets are all visible |
| Asset catalog, materialization/observation history | Only assets defined in visible locations |
| Run tag typeahead | Location tag values from hidden locations removed |

This **hides** data; it is not a security boundary. Dagster's GraphQL API wasn't designed for per-user reads, so some details can still surface, for example other tag values in the run filter typeahead, or asset keys from hidden locations that appear as upstream dependencies of visible assets. Runs that can't be tied to a location (no code location origin or repository tag) are hidden from scoped users. For a hard boundary, run a separate webserver per team with its own `workspace.yaml`.

---

## 📦 Backends

| Backend | Implementation | Status | Use Case |
|---------|---------------|--------|----------|
| **SQLite** | Peewee ORM | **Stable** | Local / Simple. Single instance only. |
| **PostgreSQL** | Peewee + `psycopg2` | **Stable** | Production. Recommended for Docker/K8s. |
| **MySQL/MariaDB** | Peewee + `mysql-connector` | **Stable** | Production. |
| **Redis** | Native `redis` | **Stable** | Session Storage + Distributed Rate Limiting. |
| **LDAP** | `ldap3` library | **Experimental** | Active Directory / OpenLDAP. Community maintained. |
| **Proxy** | Header-based | **Stable** | Authelia, OAuth2 Proxy, Traefik, Caddy. |
| **OpenID Connect**| Header-based | **Experimental** | AuthKit supports OIDC providers (Google, GitHub, Okta, Keycloak) via **Authelia**| 
---

## 🤖 Service Token (API clients)

Non-interactive clients that poll `/graphql` (Prometheus exporters, CI scripts) cannot go through the login form. Set a static service token to let them authenticate with a header instead:

```bash
export DAGSTER_AUTH_SERVICE_TOKEN=$(python -c 'import secrets; print(secrets.token_urlsafe(32))')
```

| Env var | Default | Purpose |
|---------|---------|---------|
| `DAGSTER_AUTH_SERVICE_TOKEN` | unset | Shared secret (min 32 chars). Feature is off when unset. |
| `DAGSTER_AUTH_SERVICE_TOKEN_HEADER` | `Dagster-Cloud-Api-Token` | Header carrying the token. |
| `DAGSTER_AUTH_SERVICE_TOKEN_ROLE` | `VIEWER` | Role granted to the token. |
| `DAGSTER_AUTH_SERVICE_TOKEN_USER` | `service-token` | Username shown in audit logs. |

```bash
curl -H "Dagster-Cloud-Api-Token: $DAGSTER_AUTH_SERVICE_TOKEN" \
     -H "Content-Type: application/json" \
     -d '{"query": "{ version }"}' https://dagster.yourcompany.com/graphql
```

The token goes through the same RBAC as any user, so a `VIEWER` token can run queries but every mutation is denied and audit-logged. It is a static, non-expiring credential: keep it at `VIEWER` unless the client really needs more. Session backends only (ignored in proxy mode); WebSocket subscriptions remain cookie-only.

---

## 🛠️ CLI Management

Manage users directly from the shell. Useful for CI/CD or admin tasks.

```bash
# Create a new launcher
dagster-authkit-plus add-user bob --launcher

# Reset password
dagster-authkit-plus change-password bob

# List everyone
dagster-authkit-plus list-users

# View RBAC permissions matrix
dagster-authkit-plus list-permissions
```

---

## 🔮 Roadmap

### Current (v1.0.0)

* ✅ Username/password auth (bcrypt)
* ✅ 4-level RBAC (ADMIN/EDITOR/LAUNCHER/VIEWER)
* ✅ SQLite, PostgreSQL, MySQL, Redis support
* ✅ GraphQL mutation blocking with official AST parser
* ✅ LDAP backend (experimental)
* ✅ Proxy authentication (Authelia, Caddy, Traefik)
* ✅ Kubernetes example with full SSO stack
* ✅ Helm chart for Kubernetes deployments
* ✅ Redis session revocation and rate limiting
* ✅ Centralized UI templates
* ✅ CSRF protection
* ✅ Cross-pod session revocation (DB-backed `session_version`)
* ✅ WebSocket authentication (GraphQL subscriptions)
* ✅ Dual rate-limiting (username + IP)
* ✅ Proxy trusted IP allowlist
* ✅ Token-protected `/auth/metrics`
* ✅ Service token for non-interactive API clients
* ✅ Automated `CHANGELOG.md` generation

### Next

* 🔄 OIDC backend (beyond proxy mode)

**What we will NOT do:**

* ❌ Inject React code into Dagster UI (too brittle)
* ❌ Complex enterprise features (that's what Dagster+ is for)

---

## 🤝 Contributing

Found a bug? Want to add a feature?
Open a PR. If it works and keeps things simple, we'll merge it.

**Especially needed:**

- People with Active Directory experience to validate the LDAP backend
- Testing on different Dagster versions
- Helm chart contributions

---

## 📄 License

Apache 2.0 - see [LICENSE](LICENSE)

---

## 🙏 Credits

Built by [Demetrius Albuquerque](https://github.com/demetrius-mp) because self-hosting Dagster shouldn't mean no auth. dagster-authkit-plus is maintained by Isaac Henry as a fork of [dagster-authkit](https://github.com/maltzsama/dagster-authkit).

Inspired by the community's need for a middle ground between "no auth" and "pay for Dagster+".
