"""The DBOS ``Serializer`` adapter: JSON transport for temporal-dbos
checkpoints (DESIGN §6.9), replacing DBOS's default pickle.

By the time data reaches this serializer it is already JSON-safe: every user
value has been converted to a payload dict at the Temporal boundaries
(``conversion``), and our internal envelopes (inbox messages, replies,
run-meta, failure envelopes, step results) are plain dicts/lists/scalars. So
the serializer is a thin JSON codec, with two wrinkles:

* Values and exceptions share one ``deserialize`` method (DBOS stores both in
  the same column shape), so output is tagged ``{"v": ...}`` / ``{"e": ...}``.
* Our marker exceptions (``SerializedWorkflowFailure`` / ``*Cancellation`` /
  ``SerializedContinueAsNew``) must round-trip to the *exact* class — status
  mapping does ``isinstance`` on them (status.py). DBOS's own portable-JSON
  serializer flattens every exception to ``PortableWorkflowError``, which would
  collapse CANCELED / CONTINUED_AS_NEW into FAILED; hence our own.

All processes touching one database must use a serializer with the same
``name()`` — DBOS selects the deserializer by the per-row label and refuses a
mismatch — so ``Worker`` (via ``DBOSConfig``) and ``Client`` (via the wrapped
``DBOSClient``) both install this.

One non-pre-converted value reaches this serializer: a scheduled workflow's
``fired_at`` ``datetime``, which DBOS injects as the first argument of every
schedule fire (``__temporal_schedule_fire``) and serializes through the
configured serializer with no type-hint coercion (that path is portable-JSON
only). We encode it as a plain ISO-8601 **string** (via ``default`` below) and
do NOT reconstruct it on decode — the schedule-fire dispatcher coerces its
``fired_at`` argument back to a datetime itself. Reconstructing here would mean
a global ``object_hook`` over every decoded value, which could mis-cast a user
payload that happens to match the marker shape; emitting a bare string keeps
decode lossless for all user data (user datetimes are already ISO strings in
their payload dicts by the time they reach this serializer).
"""

import json
from datetime import datetime
from typing import Any

from dbos import Serializer

from .payloads import (
    SerializedContinueAsNew,
    SerializedWorkflowCancellation,
    SerializedWorkflowFailure,
)

SERIALIZER_NAME = "temporal_dbos_json"


def _json_default(value: Any) -> Any:
    # The only raw datetime reaching us is a schedule fire's ``fired_at``; emit
    # an ISO string (the dispatcher parses it). Anything else is a real bug.
    if isinstance(value, datetime):
        return value.isoformat()
    raise TypeError(f"Object of type {type(value).__name__} is not JSON serializable")


def _exc_to_dict(exc: BaseException) -> dict[str, Any]:
    # Cancellation is a SerializedWorkflowFailure subclass — check it first.
    if isinstance(exc, SerializedContinueAsNew):
        return {"marker": "continue_as_new", "envelope": exc.envelope}
    if isinstance(exc, SerializedWorkflowCancellation):
        return {"marker": "cancellation", "envelope": exc.envelope}
    if isinstance(exc, SerializedWorkflowFailure):
        return {"marker": "failure", "envelope": exc.envelope}
    # A non-marker exception (rare — activities serialize their own failures;
    # this covers e.g. an internal step error or FAIL_FAST mode).
    return {"marker": "other", "type": type(exc).__name__, "message": str(exc)}


def _dict_to_exc(data: dict[str, Any]) -> BaseException:
    marker = data.get("marker")
    if marker == "continue_as_new":
        return SerializedContinueAsNew(data["envelope"])
    if marker == "cancellation":
        return SerializedWorkflowCancellation(data["envelope"])
    if marker == "failure":
        return SerializedWorkflowFailure(data["envelope"])
    return Exception(data.get("message", "unknown error"))


class TemporalDBOSSerializer(Serializer):
    """JSON serializer for temporal-dbos checkpoints (see module docstring)."""

    def name(self) -> str:
        return SERIALIZER_NAME

    def serialize(self, data: Any) -> str:
        if isinstance(data, BaseException):
            return json.dumps(
                {"e": _exc_to_dict(data)}, ensure_ascii=False, default=_json_default
            )
        return json.dumps({"v": data}, ensure_ascii=False, default=_json_default)

    def deserialize(self, serialized_data: str) -> Any:
        if serialized_data is None:
            return None
        obj = json.loads(serialized_data)
        if "e" in obj:
            return _dict_to_exc(obj["e"])
        return obj["v"]


# Stateless — one shared instance is enough (and keeps name() identical).
TEMPORAL_SERIALIZER = TemporalDBOSSerializer()
