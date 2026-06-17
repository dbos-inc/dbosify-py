"""Temporal namespaces backed by DBOS system schemas (DEVIATIONS D1).

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

# Default namespace, matching Temporal.
DEFAULT_NAMESPACE = "default"

# Every namespace's schema is ``temporal_<namespace>`` (no namespace is
# privileged — ``default`` maps to ``temporal_default``, not the bare ``dbos``
# schema). The prefix keeps namespace schemas clear of DBOS's own ``dbos``
# schema and of reserved words (e.g. ``default``).
_SCHEMA_PREFIX = "temporal_"

# Postgres identifiers are capped at 63 bytes; reserve room for the prefix.
_MAX_NAMESPACE_LEN = 63 - len(_SCHEMA_PREFIX)

# A namespace must be a plain lowercase identifier so it maps to an unambiguous,
# unquoted Postgres schema (case-folding and quoting hazards avoided). Temporal
# namespace names are typically already this shape.
_NAMESPACE_RE = re.compile(r"^[a-z_][a-z0-9_]*$")


def namespace_schema(namespace: str) -> str:
    """The DBOS system schema backing ``namespace``.

    Raises ``ValueError`` if the namespace can't map to a Postgres schema —
    namespaces must match ``[a-z_][a-z0-9_]*`` and fit within the identifier
    length limit once prefixed.
    """
    if not _NAMESPACE_RE.match(namespace) or len(namespace) > _MAX_NAMESPACE_LEN:
        raise ValueError(
            f"namespace {namespace!r} cannot back a Postgres schema: a namespace "
            f"must match [a-z_][a-z0-9_]* and be at most {_MAX_NAMESPACE_LEN} "
            "characters (each namespace maps to its own DBOS system schema "
            f"{_SCHEMA_PREFIX}<namespace>)"
        )
    return f"{_SCHEMA_PREFIX}{namespace}"
