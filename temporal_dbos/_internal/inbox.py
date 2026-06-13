"""The inbox: one totally-ordered durable message stream per execution.

Everything external to a workflow execution (signals, updates, queries,
cancel requests) is delivered as an envelope on a single DBOS topic via
``DBOS.send`` and consumed with ``DBOS.recv`` — which checkpoints each
consumed message, fixing delivery order at first execution so replay
delivers identically. Replies (update/query results) flow back through
``DBOS.set_event`` keyed by request id.

Envelopes are plain dicts (serialized by the DBOS serializer):
  {"kind": "signal"|"update"|"query"|"cancel", "name": str, "args": [...],
   "sent_at": float, and for updates/queries an "update_id"/"request_id"}

``sent_at`` is the sender's wall clock; the interpreter advances virtual
time to it on delivery (it rides inside recv's checkpoint, so it is
replay-stable).
"""

import asyncio
import time
from typing import Any, Dict, Sequence

INBOX_TOPIC = "__tdb_inbox"

# recv timeout per wait; on (checkpointed, deterministic) timeout the
# interpreter just re-issues the recv.
RECV_TIMEOUT_SECONDS = 3600.0

# Stale-listener recovery (see recv_resilient): attempts × delay bounds how
# long we chase an orphaned recv registration before giving up loudly.
_STALE_LISTENER_RETRIES = 100
_STALE_LISTENER_RETRY_DELAY_SECONDS = 0.05

Envelope = Dict[str, Any]


def clear_stale_listener(workflow_id: str, topic: str = INBOX_TOPIC) -> None:
    """Remove an orphaned DBOS recv listener registration for this workflow.

    DBOS's ``recv_async`` registers a listener in ``notifications_map``
    inside ``recv_setup`` (offloaded to a thread) and unregisters it in a
    try/finally that only begins *after* the setup await returns. Cancelling
    the recv task while setup is in flight — which the interpreter does to
    its parked inbox waiter at every run close — abandons the coroutine
    while the thread completes the registration, which is then never
    cleaned up. The next recv on the same workflow+topic finds the stale
    entry and DBOS misreads it as a *concurrent duplicate execution*
    (DBOSWorkflowConflictIDError), parking the run in await_workflow_result
    forever. (Upstream-worthy: recv_async should unregister on
    cancellation.)

    The registration map is reference-counted, and the conflict path itself
    increments the count before raising — so drain to zero. Safe by
    construction at our call sites: the workflow is its inbox's only
    consumer, and this only runs when no live recv of ours is outstanding.
    """
    from dbos._dbos import _get_dbos_instance

    notifications_map = _get_dbos_instance()._sys_db.notifications_map
    key = f"{workflow_id}::{topic}"
    for _ in range(64):
        if notifications_map.get(key) is None:
            return
        notifications_map.pop(key)


async def recv_resilient(
    workflow_id: str, timeout_seconds: float, topic: str = INBOX_TOPIC
) -> Any:
    """``DBOS.recv_async`` hardened against the stale-listener leak (see
    :py:func:`clear_stale_listener`): clear any orphaned registration up
    front, and if an orphaned setup thread lands its registration in the
    window between our clear and recv's own setup, clear and retry.

    A conflicted attempt claims function ids but records nothing, so replay
    after a crash-during-conflict re-executes the drain position live — no
    message is lost or double-delivered (consumption and forwards are each
    checkpointed); only the cross-recovery drain order can shift, which the
    mid-drain-crash contract already allows.
    """
    from dbos import DBOS
    from dbos._error import DBOSWorkflowConflictIDError

    clear_stale_listener(workflow_id, topic)
    for _ in range(_STALE_LISTENER_RETRIES):
        try:
            return await DBOS.recv_async(topic, timeout_seconds)
        except DBOSWorkflowConflictIDError:
            clear_stale_listener(workflow_id, topic)
            await asyncio.sleep(_STALE_LISTENER_RETRY_DELAY_SECONDS)
    raise RuntimeError(
        f"inbox recv for {workflow_id!r} kept conflicting with a stale "
        f"listener registration after {_STALE_LISTENER_RETRIES} attempts"
    )


def signal_envelope(name: str, args: Sequence[Any]) -> Envelope:
    return {
        "kind": "signal",
        "name": name,
        "args": list(args),
        "sent_at": time.time(),
    }


def update_envelope(name: str, args: Sequence[Any], update_id: str) -> Envelope:
    return {
        "kind": "update",
        "name": name,
        "args": list(args),
        "update_id": update_id,
        "sent_at": time.time(),
    }


def cancel_envelope(reason: str = "") -> Envelope:
    return {
        "kind": "cancel",
        "name": "",
        "args": [],
        "reason": reason,
        "sent_at": time.time(),
    }


def activity_result_envelope(
    activity_id: str,
    *,
    result: Any = None,
    failure: Any = None,
    cancelled: bool = False,
) -> Envelope:
    """External completion of an async activity (raise_complete_async)."""
    return {
        "kind": "activity_result",
        "name": "",
        "args": [],
        "activity_id": activity_id,
        "ok": failure is None and not cancelled,
        "result": result,
        "failure": failure,
        "cancelled": cancelled,
        "sent_at": time.time(),
    }


def activity_heartbeat_envelope(activity_id: str, details: Sequence[Any]) -> Envelope:
    return {
        "kind": "activity_heartbeat",
        "name": "",
        "args": [],
        "activity_id": activity_id,
        "details": list(details),
        "sent_at": time.time(),
    }


def async_activity_gone_key(activity_id: str) -> str:
    """Event set when a parked async activity will never accept its
    completion (cancelled, or its run closed): completers poll it so their
    heartbeats/completions can raise instead of going into the void."""
    return f"__tdb_act_{activity_id}_gone"


def query_envelope(name: str, args: Sequence[Any], request_id: str) -> Envelope:
    return {
        "kind": "query",
        "name": name,
        "args": list(args),
        "request_id": request_id,
        "sent_at": time.time(),
    }


# Durable registry of a workflow's children and their ParentClosePolicy:
# [{"id": child_id, "policy": int}, ...]. Written (checkpointed set_event) as
# children start, so parent-close policies survive the parent — including
# termination, where no workflow code runs and the *client* applies them.
CHILDREN_EVENT_KEY = "__tdb_children"


def update_acceptance_key(update_id: str) -> str:
    return f"__tdb_upd_{update_id}_accepted"


def update_result_key(update_id: str) -> str:
    return f"__tdb_upd_{update_id}"


def query_result_key(request_id: str) -> str:
    return f"__tdb_q_{request_id}"
