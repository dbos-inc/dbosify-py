"""Activity execution: one single-attempt DBOS step per activity type.

Each registered activity gets its own step function named ``act:{type}``
(decision §10.1: per-type naming keeps DBOS-native step listings readable
and makes replay-mismatch detection precise). The step body resolves the
activity from the registry at execution time, so re-registering an activity
implementation takes effect without re-decoration.

The step *always returns an envelope* — user exceptions are caught inside
the step body and serialized through the failure serializer — so checkpoint
contents are fully under our control and the interpreter's retry machinery
(not DBOS's) decides what happens next. ``ended_at`` rides in the envelope
to advance the workflow's virtual clock deterministically.
"""

import asyncio
import contextvars
import time as time_mod
from contextlib import nullcontext
from typing import Any, Callable, Coroutine, Dict, List, Optional, Tuple

from dbos import DBOS

from .. import exceptions
from ..common import RetryPolicy
from . import activity_interceptor, inbox, registry
from .payloads import FailureEnvelope, serialize_failure

AttemptStep = Callable[
    [List[Any], Optional[float], Dict[str, Any]], Coroutine[Any, Any, Dict[str, Any]]
]


def activity_api_complete_async_error() -> "type[BaseException]":
    from .. import activity as activity_api

    return activity_api._CompleteAsyncError


_attempt_steps: Dict[str, AttemptStep] = {}

# The single catch-all step (DBOS name ``act:__dynamic__``) for the dynamic
# activity, if one is registered. Any activity type with no exact step falls
# back to this — the requested type rides in ``meta["activity_type"]``, so the
# recorded step identity is stable across replay regardless of the type called.
_DYNAMIC_STEP_NAME = "__dynamic__"
_dynamic_attempt_step: Optional[AttemptStep] = None


def ensure_attempt_step(activity_name: str) -> None:
    if activity_name in _attempt_steps:
        return
    _attempt_steps[activity_name] = _make_attempt_step(activity_name)


def ensure_dynamic_attempt_step() -> None:
    global _dynamic_attempt_step
    if _dynamic_attempt_step is None:
        _dynamic_attempt_step = _make_attempt_step(_DYNAMIC_STEP_NAME, dynamic=True)


def attempt_step_for(activity_name: str) -> AttemptStep:
    step = _attempt_steps.get(activity_name)
    if step is not None:
        return step
    # Unregistered type: route to the dynamic activity if one exists.
    if _dynamic_attempt_step is not None:
        return _dynamic_attempt_step
    raise KeyError(
        f"Activity type {activity_name!r} is not registered with this worker. "
        f"Registered types: {sorted(_attempt_steps)}"
    )


def retry_decision(
    policy: RetryPolicy,
    attempt: int,
    failure: FailureEnvelope,
    *,
    elapsed: Optional[float],
    schedule_to_close: Optional[float],
) -> Tuple[Optional[float], exceptions.RetryState]:
    """The activity retry decision (DESIGN §6.1.2), clock-independent so both
    execution paths share it: the local path (interpreter, virtual time) and the
    queued path (the ``__temporal_activity`` workflow, recorded-timestamp time).

    Returns ``(backoff delay before the next attempt, or None to give up;
    the retry state to report when giving up)``. ``elapsed`` is the time since
    the activity was scheduled; pass it (with ``schedule_to_close``) so the
    schedule-to-close budget gates retries — like Temporal, it bounds the retry
    sequence, not an in-flight attempt (``start_to_close`` bounds that).
    """
    if failure.get("non_retryable"):
        return None, exceptions.RetryState.NON_RETRYABLE_FAILURE
    failure_type = failure.get("type") or failure["cls"]
    if policy.non_retryable_error_types and failure_type in set(
        policy.non_retryable_error_types
    ):
        return None, exceptions.RetryState.NON_RETRYABLE_FAILURE
    if policy.maximum_attempts and attempt >= policy.maximum_attempts:
        return None, exceptions.RetryState.MAXIMUM_ATTEMPTS_REACHED
    override = failure.get("next_retry_delay")
    if override is not None:
        delay = float(override)
    else:
        delay = policy.initial_interval.total_seconds() * (
            policy.backoff_coefficient ** (attempt - 1)
        )
        maximum = (
            policy.maximum_interval.total_seconds()
            if policy.maximum_interval
            else policy.initial_interval.total_seconds() * 100
        )
        delay = min(delay, maximum)
    if schedule_to_close is not None and elapsed is not None:
        if elapsed + delay >= schedule_to_close:
            return None, exceptions.RetryState.TIMEOUT
    return delay, exceptions.RetryState.IN_PROGRESS


class _RootActivityInbound(activity_interceptor.ActivityInboundInterceptor):
    """Root of the activity inbound chain: actually invokes the user function.

    Built fresh per attempt. ``init`` installs the (possibly interceptor-
    wrapped) outbound on the activity context so ``activity.info()`` /
    ``activity.heartbeat()`` route through it.
    """

    def __init__(self, ctx: Any, is_async: bool) -> None:
        # Intentionally not calling super().__init__: this is the chain root,
        # there is no ``next`` to delegate to.
        self._ctx = ctx
        self._is_async = is_async

    def init(self, outbound: activity_interceptor.ActivityOutboundInterceptor) -> None:
        self._ctx.outbound = outbound

    async def execute_activity(
        self, input: activity_interceptor.ExecuteActivityInput
    ) -> Any:
        state = self._ctx.worker_state
        # Cap concurrent activity execution to Worker(max_concurrent_activities=)
        # (a no-op when unset); the slot is held for the activity's whole run.
        slot = state.activity_slot() if state is not None else nullcontext()
        async with slot:
            if self._is_async:
                return await input.fn(*input.args)
            # Run the sync activity on the Worker's activity_executor if it set
            # one, else the loop default. run_in_executor does not copy
            # contextvars (unlike asyncio.to_thread), so copy the current context
            # explicitly — the activity context (info/heartbeat) rides one.
            executor = state.activity_executor if state is not None else None
            ctx = contextvars.copy_context()
            loop = asyncio.get_running_loop()
            return await loop.run_in_executor(
                executor, lambda: ctx.run(input.fn, *input.args)
            )


class _RootActivityOutbound(activity_interceptor.ActivityOutboundInterceptor):
    """Root of the activity outbound chain: the un-intercepted
    ``info()``/``heartbeat()`` behavior (defined in ``activity.py``)."""

    def __init__(self, ctx: Any) -> None:
        # Chain root; no ``next``.
        self._ctx = ctx

    def info(self) -> Any:
        return self._ctx.info

    def heartbeat(self, *details: Any) -> None:
        from .. import activity as activity_api

        activity_api._root_heartbeat(self._ctx, *details)


def _make_attempt_step(activity_name: str, *, dynamic: bool = False) -> AttemptStep:
    async def attempt(
        args: List[Any], start_to_close: Optional[float], meta: Dict[str, Any]
    ) -> Dict[str, Any]:
        from .. import activity as activity_api

        # The dynamic step handles any unmatched activity type: resolve the
        # single dynamic activity rather than one keyed by the requested name
        # (which has no registration). The real type rides in meta.
        defn = (
            registry.require_dynamic_activity()
            if dynamic
            else registry.lookup_activity(activity_name)
        )

        attempt_started_at = time_mod.time()
        attempt_key = (str(meta.get("workflow_run_id", "")), int(meta.get("seq", -1)))
        heartbeat_timeout = meta.get("heartbeat_timeout")
        # On the queued path the workflow runs in another process, so it can't
        # set our in-process cancel Event; instead it sets a checkpointed cancel
        # event on its run that we poll here (§6.1.2). Reads inside this step are
        # not recorded as workflow steps, so the polling stays replay-safe.
        queued = bool(meta.get("queued"))
        cancel_target = str(meta.get("workflow_run_id", ""))
        cancel_key = inbox.activity_cancel_key(str(meta.get("activity_id", "")))

        async def _cancel_requested() -> bool:
            from dbos import DBOS

            return bool(await DBOS.get_event_async(cancel_target, cancel_key, 0))

        # The activity context (activity.info()/heartbeat()) rides a
        # contextvar; asyncio.to_thread copies the context, so sync
        # activities see it too. Registering the context lets the
        # interpreter deliver cancellation into a running attempt (the
        # threading.Event outlives task cancellation, so an abandoned
        # sync thread still observes it at its next heartbeat).
        ctx = activity_api._Context(
            info=activity_api._make_info(meta),
            on_heartbeat=lambda *details: None,
            attempt_key=attempt_key,
            # Capture the running worker's activity state: its shutdown event
            # (activity.is_worker_shutdown()) and lazy client (activity.client()).
            worker_state=activity_api._active,
        )

        async def call_user_activity() -> Dict[str, Any]:
            from . import conversion

            if dynamic:
                # A dynamic activity receives a single Sequence[RawValue]: wrap
                # each raw payload untouched so the activity converts it itself
                # via activity.payload_converter().
                from ..common import RawValue

                raw = await conversion.decode_values(args, [RawValue] * len(args))
                decoded_args: List[Any] = [raw]
            else:
                decoded_args = await conversion.decode_values(args, defn.arg_types)
            headers = await conversion.decode_headers(meta.get("headers"))
            activity_api._register_attempt(attempt_key, ctx)
            token = activity_api._current_context.set(ctx)
            try:
                # Build the activity interceptor chain for this attempt
                # (DESIGN §6.8): inbound interceptors wrap the invocation,
                # outbound wraps activity.info()/heartbeat() (installed via
                # init()). With no configured interceptors this is just the
                # root, preserving the prior dispatch exactly.
                impl: activity_interceptor.ActivityInboundInterceptor = (
                    _RootActivityInbound(ctx, defn.is_async)
                )
                for interceptor in reversed(registry.worker_interceptors):
                    impl = interceptor.intercept_activity(impl)
                impl.init(_RootActivityOutbound(ctx))
                result = await impl.execute_activity(
                    activity_interceptor.ExecuteActivityInput(
                        fn=defn.fn, args=decoded_args, executor=None, headers=headers
                    )
                )
            except Exception as err:  # noqa: BLE001 — serialized, not swallowed
                return {
                    "ok": False,
                    "failure": serialize_failure(err),
                    "ended_at": time_mod.time(),
                }
            finally:
                activity_api._current_context.reset(token)
                activity_api._unregister_attempt(attempt_key, ctx)
            return {
                "ok": True,
                "result": await conversion.encode_value(result),
                "ended_at": time_mod.time(),
            }

        async def run_attempt() -> Dict[str, Any]:
            # A watchdog loop is needed when there's a heartbeat timeout to
            # enforce, OR on the queued path to poll for cross-process
            # cancellation. Otherwise run the activity directly.
            if heartbeat_timeout is None and not queued:
                return await call_user_activity()
            # Heartbeat-timeout watchdog (Temporal's liveness contract): an
            # attempt that stops heartbeating for longer than the timeout
            # fails with TimeoutType.HEARTBEAT (and retries per policy). The
            # hung function is marked cancelled — a still-live thread
            # unwinds at its next heartbeat — and abandoned, like
            # start-to-close enforcement.
            task = asyncio.ensure_future(call_user_activity())
            poll = (
                max(0.05, float(heartbeat_timeout) / 4)
                if heartbeat_timeout is not None
                else 0.25
            )
            try:
                return await _watch(task, poll)
            except asyncio.CancelledError:
                # asyncio.wait does NOT cancel what it waits on: propagate
                # explicitly so TRY_CANCEL / start-to-close actually stop an
                # async activity function rather than orphaning it.
                task.cancel()
                raise

        async def _watch(
            task: "asyncio.Task[Dict[str, Any]]", poll: float
        ) -> Dict[str, Any]:
            while True:
                done, _ = await asyncio.wait({task}, timeout=poll)
                if done:
                    return task.result()
                if queued and await _cancel_requested():
                    # Cross-process cancellation: deliver it into the activity
                    # (its next heartbeat raises; cancelling the task unwinds an
                    # awaiting async activity now), let its cleanup run, then
                    # report the attempt as cancelled.
                    ctx.cancelled.set()
                    task.cancel()
                    try:
                        await task
                    except asyncio.CancelledError:
                        pass
                    cancelled = exceptions.CancelledError("Activity cancelled")
                    return {
                        "ok": False,
                        "failure": serialize_failure(cancelled),
                        "ended_at": time_mod.time(),
                    }
                if not queued and defn.is_async and ctx.cancelled.is_set():
                    # Local cancellation requested by the workflow (handle.cancel()
                    # / scope cancel set ctx.cancelled via _request_cancel). A
                    # *sync* activity observes that through activity.heartbeat();
                    # an *async* one awaiting something other than a heartbeat
                    # (asyncio.Future, sleep, a child) must be cancelled at the
                    # task level, or it never unwinds and the heartbeat watchdog
                    # below fires a spurious HEARTBEAT timeout. The user may catch
                    # the CancelledError and return a value (preserved, as in
                    # Temporal).
                    task.cancel()
                    try:
                        return await task
                    except asyncio.CancelledError:
                        cancelled = exceptions.CancelledError("Activity cancelled")
                        return {
                            "ok": False,
                            "failure": serialize_failure(cancelled),
                            "ended_at": time_mod.time(),
                        }
                if heartbeat_timeout is not None:
                    stale = time_mod.monotonic() - ctx.last_heartbeat_at
                    if stale > float(heartbeat_timeout):
                        ctx.cancelled.set()
                        task.cancel()
                        hb_timeout = exceptions.TimeoutError(
                            "activity Heartbeat timeout",
                            type=exceptions.TimeoutType.HEARTBEAT,
                            last_heartbeat_details=list(ctx.last_heartbeat),
                        )
                        return {
                            "ok": False,
                            "failure": serialize_failure(hb_timeout),
                            "ended_at": time_mod.time(),
                        }

        async def run_to_deadline() -> Dict[str, Any]:
            # Enforce start-to-close OURSELVES rather than via asyncio.wait_for.
            # wait_for returns the coroutine's value if it swallows the timeout
            # cancellation — an activity that catches CancelledError and returns
            # would thereby defeat the deadline and be recorded as a success.
            # The deadline is authoritative: once it passes we cancel + abandon
            # the attempt and discard whatever it later produces, so a
            # start-to-close timeout is always surfaced (matching the
            # heartbeat-timeout watchdog above and Temporal's hard enforcement).
            if start_to_close is None:
                return await run_attempt()
            task = asyncio.ensure_future(run_attempt())
            try:
                done, _ = await asyncio.wait({task}, timeout=start_to_close)
            except asyncio.CancelledError:
                # External cancellation (workflow cancel / scope): propagate the
                # cancel into the attempt like wait_for would, then re-raise.
                task.cancel()
                raise
            if done:
                return task.result()
            task.cancel()
            try:
                await task
            except BaseException:  # noqa: BLE001 — late result/error is discarded
                pass
            raise asyncio.TimeoutError

        try:
            # User exceptions (including user-raised TimeoutError) are
            # converted inside call_user_activity, so a TimeoutError here is
            # unambiguously the start-to-close enforcement firing.
            return await run_to_deadline()
        except activity_api_complete_async_error():
            # raise_complete_async(): the function returned, but the
            # activity stays pending until externally completed (the
            # checkpointed marker makes the parked state replay-stable).
            # started_at lets the interpreter arm the *remaining*
            # start-to-close for the parked wait (per-attempt, as in
            # Temporal).
            return {
                "async_pending": True,
                "started_at": attempt_started_at,
                "ended_at": time_mod.time(),
            }
        except (asyncio.TimeoutError, TimeoutError):
            # Mark the context so a hung sync thread (which cancellation
            # cannot interrupt) still unwinds at its next heartbeat.
            ctx.cancelled.set()
            timeout_failure = exceptions.TimeoutError(
                "activity Start-To-Close timeout",
                type=exceptions.TimeoutType.START_TO_CLOSE,
                last_heartbeat_details=[],
            )
            return {
                "ok": False,
                "failure": serialize_failure(timeout_failure),
                "ended_at": time_mod.time(),
            }

    attempt.__name__ = attempt.__qualname__ = f"act:{activity_name}"
    decorated: AttemptStep = DBOS.step(name=f"act:{activity_name}")(attempt)
    return decorated
