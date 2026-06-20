"""Temporal namespaces backed by DBOS system schemas (DEVIATIONS no-server).

Each Temporal namespace maps to its own Postgres schema holding the DBOS
system tables (``dbos_system_schema``), so workflows in different namespaces
are isolated: same workflow id can exist independently in two namespaces, and
``list``/``describe`` in one never sees the other. This is cheap schema-level
isolation, not an authorization boundary (Postgres security still applies to
the database as a whole).

Because DBOS's launched runtime and its ``SystemSchema`` are process-global,
a process serves exactly one namespace: a ``Worker`` derives its
``dbos_system_schema`` from its ``namespace`` (the Worker owns the runtime),
and a ``Client`` validates that its ``DBOSClient`` was built with the matching
schema. Different namespaces therefore mean different worker processes /
``DBOSClient``\\ s — mirroring Temporal, where a worker polls one namespace.
"""

import re
from typing import Optional

# Default namespace, matching Temporal.
DEFAULT_NAMESPACE = "default"

# Every namespace's schema is ``dbosify_<namespace>`` (``default`` maps to
# ``dbosify_default``); the prefix keeps these clear of DBOS's own ``dbos`` schema.
_SCHEMA_PREFIX = "dbosify_"

# Postgres identifiers are capped at 63 bytes; reserve room for the prefix.
_MAX_NAMESPACE_LEN = 63 - len(_SCHEMA_PREFIX)

# A namespace must be a plain lowercase identifier so it maps to an unambiguous,
# unquoted Postgres schema (avoiding case-folding and quoting hazards).
_NAMESPACE_RE = re.compile(r"^[a-z_][a-z0-9_]*$")


def namespace_schema(namespace: str) -> str:
    """The DBOS system schema backing ``namespace``.

    Raises ``ValueError`` if the namespace can't map to a Postgres schema —
    namespaces must match ``[a-z_][a-z0-9_]*`` and fit within the identifier
    length limit once prefixed.
    """
    if not _NAMESPACE_RE.match(namespace) or len(namespace) > _MAX_NAMESPACE_LEN:
        raise ValueError(
            f"namespace {namespace!r} cannot back a Postgres schema: it must match "
            f"[a-z_][a-z0-9_]* and be at most {_MAX_NAMESPACE_LEN} characters"
        )
    return f"{_SCHEMA_PREFIX}{namespace}"


def namespace_from_schema(schema: Optional[str]) -> str:
    """The namespace whose DBOS system schema is ``schema`` — the inverse of
    :func:`namespace_schema`. Used by the low-level ``Client(dbos_client)``
    path, where the DBOSClient's schema is the single source of truth.

    Raises ``ValueError`` if ``schema`` isn't a valid namespace schema
    (``dbosify_<namespace>``) — e.g. a bare ``dbos`` schema (or ``None``, as on
    SQLite) has no namespace.
    """
    if schema is not None and schema.startswith(_SCHEMA_PREFIX):
        namespace = schema[len(_SCHEMA_PREFIX) :]
        if _NAMESPACE_RE.match(namespace) and len(namespace) <= _MAX_NAMESPACE_LEN:
            return namespace
    raise ValueError(
        f"DBOS system schema {schema!r} is not a valid namespace schema "
        f"({_SCHEMA_PREFIX}<namespace>); use Client.connect(system_database_url, "
        "namespace=...) or build the DBOSClient with "
        "dbos_system_schema=namespace_schema(<namespace>)"
    )
