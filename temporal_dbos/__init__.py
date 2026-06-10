"""A drop-in replacement for the Temporal Python SDK (``temporalio``), backed by
DBOS Transact (Postgres) instead of a Temporal server.

Modules mirror ``temporalio``'s layout: ``temporal_dbos.workflow``,
``.activity``, ``.client``, ``.worker``, ``.common``, ``.exceptions``,
``.converter``, ``.testing``. Migration from Temporal is an import-root swap.

See DESIGN.md for the architecture and compatibility contract.
"""
