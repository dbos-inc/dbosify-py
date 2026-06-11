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

import time
from typing import Any, Dict, Sequence

INBOX_TOPIC = "__tdb_inbox"

# recv timeout per wait; on (checkpointed, deterministic) timeout the
# interpreter just re-issues the recv.
RECV_TIMEOUT_SECONDS = 3600.0

Envelope = Dict[str, Any]


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


def update_result_key(update_id: str) -> str:
    return f"__tdb_upd_{update_id}"


def query_result_key(request_id: str) -> str:
    return f"__tdb_q_{request_id}"
