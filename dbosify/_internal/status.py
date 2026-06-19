"""DBOS workflow status -> Temporal ``WorkflowExecutionStatus`` (DESIGN §6.2).

The enum lives here (re-exported by ``dbosify.client``) so internal
modules can map statuses without importing the client facade.
"""

from enum import IntEnum
from typing import Optional


class WorkflowExecutionStatus(IntEnum):
    """Status of a workflow execution, mirroring
    ``temporalio.client.WorkflowExecutionStatus``.
    """

    RUNNING = 1
    COMPLETED = 2
    FAILED = 3
    CANCELED = 4
    TERMINATED = 5
    CONTINUED_AS_NEW = 6
    TIMED_OUT = 7


_OPEN_DBOS_STATUSES = ("PENDING", "ENQUEUED", "DELAYED")


def to_execution_status(
    dbos_status: Optional[str], *, error: Optional[BaseException] = None
) -> WorkflowExecutionStatus:
    """Map a DBOS status string per the DESIGN §6.2 table. For ERROR, the
    recorded error distinguishes cooperative cancellation (CANCELED) from
    failure; pass it when available.

    Notes: ERROR timeout markers (-> TIMED_OUT) land with workflow run
    timeouts (Phase 3). MAX_RECOVERY_ATTEMPTS_EXCEEDED maps to RUNNING: the
    workflow is stuck, not closed (documented deviation).
    """
    from .payloads import SerializedContinueAsNew, SerializedWorkflowCancellation

    if dbos_status in _OPEN_DBOS_STATUSES:
        return WorkflowExecutionStatus.RUNNING
    if dbos_status == "SUCCESS":
        return WorkflowExecutionStatus.COMPLETED
    if dbos_status == "ERROR":
        if isinstance(error, SerializedContinueAsNew):
            return WorkflowExecutionStatus.CONTINUED_AS_NEW
        if isinstance(error, SerializedWorkflowCancellation):
            return WorkflowExecutionStatus.CANCELED
        return WorkflowExecutionStatus.FAILED
    if dbos_status == "CANCELLED":
        # Native DBOS cancel is reserved for terminate (decision §10.4).
        return WorkflowExecutionStatus.TERMINATED
    if dbos_status == "MAX_RECOVERY_ATTEMPTS_EXCEEDED":
        return WorkflowExecutionStatus.RUNNING
    raise ValueError(f"Unknown DBOS workflow status: {dbos_status!r}")


def is_open(dbos_status: Optional[str]) -> bool:
    return dbos_status in _OPEN_DBOS_STATUSES
