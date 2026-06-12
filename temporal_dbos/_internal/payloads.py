"""Serialized envelope formats stored in DBOS checkpoints.

The failure envelope is the stable, bidirectional serialization of the
Temporal exception tree (DESIGN.md §6.3): exception -> plain dict -> equal
exception. It is used for activity results, workflow results, and (later)
child-workflow errors, so reconstruction is exact across processes — pickle
of exception objects loses ``__cause__`` chains, envelopes don't.

Arbitrary (non-FailureError) exceptions convert to ``ApplicationError`` with
``type`` set to the original class name, mirroring temporalio's default
failure converter.
"""

import traceback
from datetime import timedelta
from typing import Any, Dict, Optional

from .. import exceptions

FailureEnvelope = Dict[str, Any]


class FailureView:
    """Attribute view over a failure envelope, shaped like the commonly-used
    fields of temporalio's protobuf ``Failure`` (``message``,
    ``stack_trace``, ``cause``). Exposed via ``FailureError.failure``.
    """

    def __init__(self, envelope: FailureEnvelope) -> None:
        self._envelope = envelope

    @property
    def message(self) -> str:
        return str(self._envelope.get("message", ""))

    @property
    def stack_trace(self) -> str:
        return str(self._envelope.get("stack_trace", ""))

    @property
    def cause(self) -> Optional["FailureView"]:
        cause = self._envelope.get("cause")
        return FailureView(cause) if cause is not None else None

    def __repr__(self) -> str:
        return f"FailureView({self._envelope!r})"


class SerializedWorkflowFailure(Exception):
    """The form a workflow failure takes in the DBOS ledger.

    The dispatcher wraps workflow ``FailureError`` outcomes in this before
    they reach DBOS's error recording, because pickling exception objects
    drops ``__cause__`` chains. Clients catch it from ``get_result`` and
    reconstruct the exact failure via ``deserialize_failure``.
    """

    def __init__(self, envelope: FailureEnvelope) -> None:
        # The envelope is the sole constructor arg so default exception
        # pickling round-trips it.
        super().__init__(envelope)
        self.envelope = envelope

    def __str__(self) -> str:
        return str(self.envelope.get("message", "workflow failed"))


class SerializedWorkflowCancellation(SerializedWorkflowFailure):
    """The ``_TemporalCancelledMarker`` of DESIGN §6.2: a workflow that ended
    via *cooperative cancellation* records this subclass, so status mapping
    can distinguish CANCELED (this, recorded by the dispatcher) from FAILED
    (plain SerializedWorkflowFailure) and TERMINATED (native DBOS cancel,
    which records nothing because no workflow code runs).
    """


class SerializedContinueAsNew(Exception):
    """Recorded as the run's DBOS "error" when it continues as new (like the
    cancellation marker): status maps to CONTINUED_AS_NEW and awaiters hop
    the chain. Deliberately NOT a SerializedWorkflowFailure subclass so no
    failure-handling clause swallows it. The envelope carries
    ``{"new_run_id": str}``.
    """

    def __init__(self, envelope: Dict[str, Any]) -> None:
        super().__init__(envelope)
        self.envelope = envelope


def serialize_failure(exc: BaseException) -> FailureEnvelope:
    env: FailureEnvelope
    if isinstance(exc, exceptions.ApplicationError):
        env = {
            "cls": "ApplicationError",
            "message": exc.message,
            "type": exc.type,
            "details": list(exc.details),
            "non_retryable": exc.non_retryable,
            "next_retry_delay": (
                exc.next_retry_delay.total_seconds()
                if exc.next_retry_delay is not None
                else None
            ),
        }
    elif isinstance(exc, exceptions.CancelledError):
        env = {
            "cls": "CancelledError",
            "message": exc.message,
            "details": list(exc.details),
        }
    elif isinstance(exc, exceptions.TerminatedError):
        env = {
            "cls": "TerminatedError",
            "message": exc.message,
            "details": list(exc.details),
        }
    elif isinstance(exc, exceptions.TimeoutError):
        env = {
            "cls": "TimeoutError",
            "message": exc.message,
            "timeout_type": int(exc.type) if exc.type is not None else None,
            "last_heartbeat_details": list(exc.last_heartbeat_details),
        }
    elif isinstance(exc, exceptions.ActivityError):
        env = {
            "cls": "ActivityError",
            "message": exc.message,
            "activity_type": exc.activity_type,
            "activity_id": exc.activity_id,
            "identity": exc.identity,
            "retry_state": (
                int(exc.retry_state) if exc.retry_state is not None else None
            ),
        }
    elif isinstance(exc, exceptions.ChildWorkflowError):
        env = {
            "cls": "ChildWorkflowError",
            "message": exc.message,
            "namespace": exc.namespace,
            "workflow_id": exc.workflow_id,
            "run_id": exc.run_id,
            "workflow_type": exc.workflow_type,
            "retry_state": (
                int(exc.retry_state) if exc.retry_state is not None else None
            ),
        }
    else:
        # Anything else (including FailureError subclasses we don't model
        # structurally) becomes an ApplicationError keyed by class name.
        env = {
            "cls": "ApplicationError",
            "message": str(exc),
            "type": exc.__class__.__name__,
            "details": [],
            "non_retryable": False,
            "next_retry_delay": None,
        }
    if exc.__traceback__ is not None:
        env["stack_trace"] = "".join(traceback.format_tb(exc.__traceback__))
    cause = exc.__cause__
    env["cause"] = serialize_failure(cause) if cause is not None else None
    return env


def deserialize_failure(env: FailureEnvelope) -> exceptions.FailureError:
    cls = env["cls"]
    exc: exceptions.FailureError
    if cls == "ApplicationError":
        delay = env.get("next_retry_delay")
        exc = exceptions.ApplicationError(
            env["message"],
            *env.get("details", []),
            type=env.get("type"),
            non_retryable=bool(env.get("non_retryable", False)),
            next_retry_delay=timedelta(seconds=delay) if delay is not None else None,
        )
    elif cls == "CancelledError":
        exc = exceptions.CancelledError(env["message"], *env.get("details", []))
    elif cls == "TerminatedError":
        exc = exceptions.TerminatedError(env["message"], *env.get("details", []))
    elif cls == "TimeoutError":
        timeout_type = env.get("timeout_type")
        exc = exceptions.TimeoutError(
            env["message"],
            type=(
                exceptions.TimeoutType(timeout_type)
                if timeout_type is not None
                else None
            ),
            last_heartbeat_details=env.get("last_heartbeat_details", []),
        )
    elif cls == "ActivityError":
        exc = exceptions.ActivityError(
            env["message"],
            scheduled_event_id=0,
            started_event_id=0,
            identity=env.get("identity", ""),
            activity_type=env["activity_type"],
            activity_id=env["activity_id"],
            retry_state=_retry_state(env.get("retry_state")),
        )
    elif cls == "ChildWorkflowError":
        exc = exceptions.ChildWorkflowError(
            env["message"],
            namespace=env.get("namespace", ""),
            workflow_id=env["workflow_id"],
            run_id=env.get("run_id", ""),
            workflow_type=env["workflow_type"],
            initiated_event_id=0,
            started_event_id=0,
            retry_state=_retry_state(env.get("retry_state")),
        )
    else:
        raise ValueError(f"unknown failure envelope class: {cls!r}")
    cause = env.get("cause")
    if cause is not None:
        exc.__cause__ = deserialize_failure(cause)
    exc._failure = FailureView(env)  # noqa: SLF001 — our own class
    return exc


def _retry_state(value: Optional[int]) -> Optional[exceptions.RetryState]:
    return exceptions.RetryState(value) if value is not None else None
