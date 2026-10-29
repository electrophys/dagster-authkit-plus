"""
Authentication engine for dagster-authkit-plus.

Contains:
- session.py      — SessionManager (signed cookies or Redis)
- security.py     — SecurityHardening (password hashing, CSRF, headers)
- rate_limiter.py — Rate limiter (in-memory or Redis)
- backends/       — Pluggable auth backends (SQL, LDAP, proxy, dummy, oauth)
"""
