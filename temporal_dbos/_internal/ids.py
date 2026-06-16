"""Temporal workflow IDs vs DBOS workflow IDs (DESIGN.md §6.4).

DBOS allows exactly one execution per DBOS workflow id, ever; Temporal
allows reusing a workflow id across *closed* runs, each run having its own
run id. Scheme (resolved decision §10.3):

  - run n of Temporal id W has DBOS id ``W`` for n=0, else ``W--r{n}``
  - the user-visible ``run_id`` IS that DBOS id (stable and unique;
    synthesizing UUIDs would add nothing)
  - user workflow ids containing the separator are rejected outright
"""

from typing import Any, Awaitable, Callable, Dict, List, Optional, Sequence, Tuple

RUN_SEPARATOR = "--r"
# Cross-queue activity workflow ids are ``{run}--a{seq}`` (§6.1.2). Reserving
# this separator (like ``--r``) keeps an activity workflow id from ever
# colliding with a user-chosen or auto-generated workflow/child id, which would
# otherwise make the idempotent ``SetWorkflowID`` enqueue silently re-attach to
# an unrelated workflow.
ACTIVITY_SEPARATOR = "--a"

# Chain resolution probes (see resolve_latest_run): chains up to this many
# runs resolve in a single batched primary-key lookup.
DENSE_PROBE_LIMIT = 16
# Exponential ladder ceiling: 2**20 ≈ 1M runs; longer chains keep doubling.
MAX_PROBE_EXPONENT = 20
# Evenly-spaced probes per refinement round (log base for convergence).
REFINE_BATCH = 16


def validate_workflow_id(workflow_id: str) -> None:
    if not workflow_id:
        raise ValueError("Workflow id must be non-empty")
    for separator in (RUN_SEPARATOR, ACTIVITY_SEPARATOR):
        if separator in workflow_id:
            raise ValueError(
                f"Workflow ids may not contain {separator!r} "
                f"(reserved for temporal-dbos internal ids): {workflow_id!r}"
            )


def run_dbos_id(workflow_id: str, run_index: int) -> str:
    """The DBOS workflow id for run ``run_index`` of a Temporal workflow id."""
    if run_index == 0:
        return workflow_id
    return f"{workflow_id}{RUN_SEPARATOR}{run_index}"


def activity_dbos_id(run_id: str, seq: int) -> str:
    """The ``__temporal_activity`` workflow id for a cross-queue activity (§6.1.2).
    Deterministic in ``seq``, and in the reserved ``--a`` namespace so it cannot
    collide with any user/child/run id."""
    return f"{run_id}{ACTIVITY_SEPARATOR}{seq}"


def parse_run(dbos_id: str) -> "tuple[str, int]":
    """Split a DBOS run id into (workflow_id, run_index).

    User workflow ids may not contain the separator, but *auto-generated
    child ids may*: a child of run ``W--r2`` is ``W--r2_5``. The suffix
    must be pure digits to count as a run index — and ``str.isdigit`` is
    the load-bearing check, because ``int("2_5")`` happily parses
    underscore-separated digits as 25.
    """
    if RUN_SEPARATOR in dbos_id:
        base, _, suffix = dbos_id.rpartition(RUN_SEPARATOR)
        if suffix.isdigit():
            return base, int(suffix)
    return dbos_id, 0


def run_index_of(workflow_id: str, dbos_id: str) -> Optional[int]:
    """If ``dbos_id`` is a run of ``workflow_id``, its run index; else None."""
    if dbos_id == workflow_id:
        return 0
    if not dbos_id.startswith(workflow_id + RUN_SEPARATOR):
        return None
    suffix = dbos_id[len(workflow_id) + len(RUN_SEPARATOR) :]
    if suffix.isdigit():
        return int(suffix)
    return None


# A batched exact-id status lookup: dbos ids -> status object per id found.
# Backed by ``list_workflows(workflow_ids=[...])`` — primary-key lookups, so
# every probe is index-backed.
ChainLookup = Callable[[Sequence[str]], Awaitable[Dict[str, Any]]]


async def resolve_latest_run(
    workflow_id: str, lookup: ChainLookup
) -> Optional[Tuple[int, Any]]:
    """The chain's newest run ``(index, status)`` — or None if no run exists
    — using exact-id lookups only.

    ``list_workflows(workflow_id_prefix=...)`` is an unindexed scan in DBOS,
    unsafe on the send/query/result critical path. But run ids are
    deterministic and dense (``W``, ``W--r1``, ..., ``W--r{n}``, gapless by
    construction), so resolution is "largest n whose exact id exists":

    1. One batched probe over a ladder — indexes 0..16 densely, then powers
       of two — answers chains up to DENSE_PROBE_LIMIT runs in a single
       round trip and brackets longer ones (front gaps from operator
       deletion/GC of old runs are tolerated: the max found anchors).
    2. Longer chains refine the bracket with REFINE_BATCH evenly spaced
       probes per round (a 500k-run chain resolves in ~5 round trips).
    """
    ladder = list(range(DENSE_PROBE_LIMIT + 1)) + [
        1 << e for e in range(5, MAX_PROBE_EXPONENT + 1)
    ]

    async def probe(indexes: List[int]) -> Dict[int, Any]:
        by_id = {run_dbos_id(workflow_id, i): i for i in indexes}
        statuses = await lookup(list(by_id))
        return {by_id[did]: status for did, status in statuses.items()}

    found = await probe(ladder)
    if not found:
        return None
    lo = max(found)
    lo_status = found[lo]
    # Exclusive upper bound: the smallest probed index above lo that was
    # missing. None means lo was the ladder top: keep doubling (absurdly
    # long chain) until the bracket closes.
    hi = next((c for c in ladder if c > lo and c not in found), None)
    while hi is None:
        extension = [lo << k for k in range(1, 5)]
        found = await probe(extension)
        missing = [c for c in extension if c not in found]
        if found:
            lo = max(found)
            lo_status = found[lo]
        hi = next((c for c in missing if c > lo), None)
    while hi - lo > 1:
        step = max(1, (hi - lo) // (REFINE_BATCH + 1))
        points = list(range(lo + step, hi, step))[:REFINE_BATCH]
        found = await probe(points)
        if found:
            lo = max(found)
            lo_status = found[lo]
            hi = next((p for p in points if p > lo and p not in found), hi)
        else:
            hi = points[0]
    return lo, lo_status
