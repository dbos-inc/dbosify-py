"""Serialized envelope formats stored in DBOS checkpoints.

The failure envelope is the stable, bidirectional serialization of the
Temporal exception tree (DESIGN.md §6.3): exception -> plain dict -> equal
exception. It is used for activity results, workflow results, and (later)
child-workflow errors, so reconstruction is exact across processes — pickle
of exception objects loses ``__cause__`` chains, envelopes don't.

Arbitrary (non-FailureError) exceptions convert to ``ApplicationError`` with
``type`` set to the original class name, mirroring temporalio's default
failure converter.

The input envelope wraps a run's start arguments with per-run metadata when
there is any (cron chains, workflow retries); plain starts keep passing the
bare args list, so all pre-envelope checkpoints stay readable. Meta keys:

  ``cron``            cron expression — this run is part of a cron chain
  ``attempt``         workflow-retry attempt, 1-based (absent = 1)
  ``retry_policy``    serialized workflow RetryPolicy (see below)
  ``run_timeout``     per-run timeout in seconds, re-applied to every chain
                      successor (DBOS would otherwise propagate the closing
                      run's *absolute* deadline to in-workflow-started
                      children — see dispatcher._enqueue_next_run)
  ``last_completion`` ``{"value": ...}`` from the chain's last successful
                      run, or None — present-ness distinguishes "no previous
                      completion" from "the result was None", as in Temporal
  ``last_failure``    failure envelope of the previous run, or None
"""

import traceback
from dataclasses import dataclass
from datetime import timedelta
from typing import Any, Dict, List, Optional, Sequence, Tuple

from .. import exceptions
from ..common import RetryPolicy

FailureEnvelope = Dict[str, Any]

INPUT_ENVELOPE_KEY = "__dbosify_input__"


@dataclass
class RunMeta:
    """Per-run metadata carried in the input envelope (and forward across
    chain hops: continue-as-new, cron continuation, workflow retries)."""

    cron: Optional[str] = None
    attempt: int = 1
    retry_policy: Optional[Dict[str, Any]] = None
    run_timeout: Optional[float] = None
    last_completion: Optional[Dict[str, Any]] = None
    last_failure: Optional[FailureEnvelope] = None
    # The encoded DBOS-attributes dict (memo + search attributes, see
    # _internal/attributes.py). Carried into the run for in-workflow info()/
    # memo() and forward across chain hops; the durable searchable copy lives in
    # the DBOS attributes column.
    attributes: Optional[Dict[str, Any]] = None
    # The run's interceptor headers in wire form (str -> payload dict). Set from
    # the client start (or a child/continue-as-new), surfaced to workflow
    # interceptors as ExecuteWorkflowInput.headers (DEVIATIONS D24). Carried
    # across cron/retry hops (the same run re-running); continue-as-new sets its
    # own (an interceptor re-injects them).
    headers: Optional[Dict[str, Any]] = None
    # The root workflow of this run's tree ({"workflow_id", "run_id"}), set when
    # this run was started as a child of another workflow; None for a top-level
    # workflow (surfaced as workflow.info().root, §6.6). Carries across chain hops
    # (a child that continues-as-new keeps the same root).
    root: Optional[Dict[str, str]] = None

    def is_empty(self) -> bool:
        return (
            self.cron is None
            and self.attempt == 1
            and self.retry_policy is None
            and self.run_timeout is None
            and self.last_completion is None
            and self.last_failure is None
            and self.attributes is None
            and not self.headers
            and self.root is None
        )

    def carried_forward(self) -> "RunMeta":
        """The meta a chain successor inherits before outcome-specific
        fields are filled in: configuration carries, attempt resets."""
        return RunMeta(
            cron=self.cron,
            attempt=1,
            retry_policy=self.retry_policy,
            run_timeout=self.run_timeout,
            last_completion=self.last_completion,
            last_failure=self.last_failure,
            attributes=self.attributes,
            headers=self.headers,
            root=self.root,
        )


def wrap_input(args: Sequence[Any], meta: Optional[RunMeta] = None) -> Any:
    """The dispatcher payload for a run: a bare args list when there is no
    metadata (the original format), else the input envelope."""
    if meta is None or meta.is_empty():
        return list(args)
    return {
        INPUT_ENVELOPE_KEY: 1,
        "args": list(args),
        "meta": {
            "cron": meta.cron,
            "attempt": meta.attempt,
            "retry_policy": meta.retry_policy,
            "run_timeout": meta.run_timeout,
            "last_completion": meta.last_completion,
            "last_failure": meta.last_failure,
            "attributes": meta.attributes,
            "headers": meta.headers,
            "root": meta.root,
        },
    }


def unwrap_input(payload: Any) -> Tuple[List[Any], RunMeta]:
    if isinstance(payload, dict) and INPUT_ENVELOPE_KEY in payload:
        raw = payload.get("meta") or {}
        return list(payload["args"]), RunMeta(
            cron=raw.get("cron"),
            attempt=int(raw.get("attempt", 1)),
            retry_policy=raw.get("retry_policy"),
            run_timeout=raw.get("run_timeout"),
            last_completion=raw.get("last_completion"),
            last_failure=raw.get("last_failure"),
            attributes=raw.get("attributes"),
            headers=raw.get("headers"),
            root=raw.get("root"),
        )
    return list(payload), RunMeta()


def serialize_retry_policy(policy: RetryPolicy) -> Dict[str, Any]:
    return {
        "initial_interval": policy.initial_interval.total_seconds(),
        "backoff_coefficient": policy.backoff_coefficient,
        "maximum_interval": (
            policy.maximum_interval.total_seconds()
            if policy.maximum_interval is not None
            else None
        ),
        "maximum_attempts": policy.maximum_attempts,
        "non_retryable_error_types": (
            list(policy.non_retryable_error_types)
            if policy.non_retryable_error_types is not None
            else None
        ),
    }


def deserialize_retry_policy(env: Dict[str, Any]) -> RetryPolicy:
    maximum = env.get("maximum_interval")
    types = env.get("non_retryable_error_types")
    return RetryPolicy(
        initial_interval=timedelta(seconds=env["initial_interval"]),
        backoff_coefficient=env["backoff_coefficient"],
        maximum_interval=timedelta(seconds=maximum) if maximum is not None else None,
        maximum_attempts=env.get("maximum_attempts", 0),
        non_retryable_error_types=list(types) if types is not None else None,
    )


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


def serialize_failure(
    exc: BaseException, converter: Optional[Any] = None
) -> FailureEnvelope:
    # Encode embedded user values (details, heartbeat details) through the
    # converter so failure envelopes are JSON-safe. Sync (no codec — like
    # query results); serialize_failure runs in deep sync call sites.
    # ``converter`` overrides the process converter (an AsyncActivityHandle
    # per-handle data converter when failing an activity).
    from . import conversion

    env: FailureEnvelope
    if isinstance(exc, exceptions.ApplicationError):
        env = {
            "cls": "ApplicationError",
            "message": exc.message,
            "type": exc.type,
            "details": conversion.encode_values_sync(list(exc.details), converter),
            "non_retryable": exc.non_retryable,
            "category": int(exc.category),
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
            "details": conversion.encode_values_sync(list(exc.details), converter),
        }
    elif isinstance(exc, exceptions.TerminatedError):
        env = {
            "cls": "TerminatedError",
            "message": exc.message,
            "details": conversion.encode_values_sync(list(exc.details), converter),
        }
    elif isinstance(exc, exceptions.TimeoutError):
        env = {
            "cls": "TimeoutError",
            "message": exc.message,
            "timeout_type": int(exc.type) if exc.type is not None else None,
            "last_heartbeat_details": conversion.encode_values_sync(
                list(exc.last_heartbeat_details), converter
            ),
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
    env["cause"] = serialize_failure(cause, converter) if cause is not None else None
    return env


def deserialize_failure(env: FailureEnvelope) -> exceptions.FailureError:
    cls = env["cls"]
    exc: exceptions.FailureError
    if cls == "ApplicationError":
        delay = env.get("next_retry_delay")
        category = env.get("category")
        exc = exceptions.ApplicationError(
            env["message"],
            *_decode_details(env.get("details", [])),
            type=env.get("type"),
            non_retryable=bool(env.get("non_retryable", False)),
            next_retry_delay=timedelta(seconds=delay) if delay is not None else None,
            category=(
                exceptions.ApplicationErrorCategory(category)
                if category is not None
                else exceptions.ApplicationErrorCategory.UNSPECIFIED
            ),
        )
    elif cls == "CancelledError":
        exc = exceptions.CancelledError(
            env["message"], *_decode_details(env.get("details", []))
        )
    elif cls == "TerminatedError":
        exc = exceptions.TerminatedError(
            env["message"], *_decode_details(env.get("details", []))
        )
    elif cls == "TimeoutError":
        timeout_type = env.get("timeout_type")
        exc = exceptions.TimeoutError(
            env["message"],
            type=(
                exceptions.TimeoutType(timeout_type)
                if timeout_type is not None
                else None
            ),
            last_heartbeat_details=_decode_details(
                env.get("last_heartbeat_details", [])
            ),
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


def _decode_details(items: Sequence[Any]) -> List[Any]:
    """Decode failure detail payloads back to user values (no codec, no hint —
    details are an untyped bag, as in temporalio)."""
    from . import conversion

    return [conversion.decode_value_sync(d) for d in items]
