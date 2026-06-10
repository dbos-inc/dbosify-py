"""Temporal workflow IDs vs DBOS workflow IDs (DESIGN.md §6.4).

DBOS allows exactly one execution per DBOS workflow id, ever; Temporal
allows reusing a workflow id across *closed* runs, each run having its own
run id. Scheme (resolved decision §10.3):

  - run n of Temporal id W has DBOS id ``W`` for n=0, else ``W--r{n}``
  - the user-visible ``run_id`` IS that DBOS id (stable and unique;
    synthesizing UUIDs would add nothing)
  - user workflow ids containing the separator are rejected outright
"""

from typing import Optional

RUN_SEPARATOR = "--r"


def validate_workflow_id(workflow_id: str) -> None:
    if not workflow_id:
        raise ValueError("Workflow id must be non-empty")
    if RUN_SEPARATOR in workflow_id:
        raise ValueError(
            f"Workflow ids may not contain {RUN_SEPARATOR!r} "
            f"(reserved for temporal-dbos run-chain ids): {workflow_id!r}"
        )


def run_dbos_id(workflow_id: str, run_index: int) -> str:
    """The DBOS workflow id for run ``run_index`` of a Temporal workflow id."""
    if run_index == 0:
        return workflow_id
    return f"{workflow_id}{RUN_SEPARATOR}{run_index}"


def run_index_of(workflow_id: str, dbos_id: str) -> Optional[int]:
    """If ``dbos_id`` is a run of ``workflow_id``, its run index; else None.

    Used to filter ``list_workflows(workflow_id_prefix=workflow_id)`` results
    down to exact chain members (the prefix also matches e.g. "{id}x").
    """
    if dbos_id == workflow_id:
        return 0
    if not dbos_id.startswith(workflow_id + RUN_SEPARATOR):
        return None
    suffix = dbos_id[len(workflow_id) + len(RUN_SEPARATOR) :]
    if suffix.isdigit():
        return int(suffix)
    return None
