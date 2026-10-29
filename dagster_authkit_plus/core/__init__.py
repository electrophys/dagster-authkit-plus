"""
Core engine for dagster-authkit-plus.

Contains:
- middleware.py       — Pure ASGI DagsterAuthMiddleware (HTTP + WebSocket)
- patch.py            — Monkey-patches DagsterWebserver to inject auth
- registry.py         — BackendRegistry with entry-point discovery
- graphql_analyzer.py — GraphQL mutation RBAC via AST parsing
- detection_layer.py  — Dagster API compatibility checks
"""
