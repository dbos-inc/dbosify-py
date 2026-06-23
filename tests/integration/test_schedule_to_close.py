"""``schedule_to_close_timeout`` bounds the whole activity lifecycle — including a
*running* attempt, not just the gaps between retries.

Before the fix, an activity with only ``schedule_to_close`` set whose attempt hung
had no deadline anywhere and hung the workflow forever; Temporal fails it with a
SCHEDULE_TO_CLOSE timeout. These cover: a hang with only schedule_to_close, a
schedule_to_close shorter than start_to_close cutting the attempt early, and the
retry-exhaustion give-up surfacing a SCHEDULE_TO_CLOSE cause that nests the last
failure.
"""

import asyncio
import time as _time
from contextlib import asynccontextmanager
from datetime import timedelta
from typing import AsyncIterator, List

import pytest

from dbosify import activity, workflow
from dbosify.client import Client
from dbosify.common import RetryPolicy
from dbosify.exceptions import ActivityError, ApplicationError, TimeoutError
from dbosify.worker import Worker
from tests.dbconfig import connect_client, default_config

pytestmark = pytest.mark.usefixtures("dbosify_env")

TASK_QUEUE = "stc-tq"


@activity.defn
async def hang_forever() -> None:
    await asyncio.Event().wait()


@activity.defn
async def always_fails() -> None:
    raise ApplicationError("boom", type="Boom")


# Records each invocation of the parked async activity below (must run exactly once under a budget timeout).
_PARK_INVOCATIONS: List[int] = []


@activity.defn
async def park_and_count() -> None:
    _PARK_INVOCATIONS.append(activity.info().attempt)
    activity.raise_complete_async()


def _describe(err: ActivityError) -> str:
    """A flat, serializable summary of the terminal ActivityError so the
    assertion lives in the test, not the workflow."""
    cause = err.cause
    state = err.retry_state.name if err.retry_state is not None else "?"
    if not isinstance(cause, TimeoutError):
        return f"other:{type(cause).__name__}:state={state}"
    ttype = cause.type.name if cause.type is not None else "?"
    inner = type(cause.__cause__).__name__ if cause.__cause__ is not None else "None"
    return f"timeout:{ttype}:inner={inner}:state={state}"


@workflow.defn
class OnlyScheduleToCloseHang:
    @workflow.run
    async def run(self) -> str:
        try:
            await workflow.execute_activity(
                hang_forever,
                schedule_to_close_timeout=timedelta(seconds=2),
            )
            return "no-timeout"
        except ActivityError as err:
            return _describe(err)


@workflow.defn
class ScheduleToCloseBeatsStartToClose:
    @workflow.run
    async def run(self) -> str:
        try:
            await workflow.execute_activity(
                hang_forever,
                start_to_close_timeout=timedelta(seconds=60),
                schedule_to_close_timeout=timedelta(seconds=2),
            )
            return "no-timeout"
        except ActivityError as err:
            return _describe(err)


@workflow.defn
class RetryExhaustsBudget:
    @workflow.run
    async def run(self) -> str:
        try:
            await workflow.execute_activity(
                always_fails,
                schedule_to_close_timeout=timedelta(seconds=2),
                retry_policy=RetryPolicy(
                    initial_interval=timedelta(seconds=0.4),
                    backoff_coefficient=1.0,
                    maximum_attempts=100,  # the budget, not the count, must stop it
                ),
            )
            return "no-timeout"
        except ActivityError as err:
            return _describe(err)


@workflow.defn
class ParkedAsyncScheduleToClose:
    @workflow.run
    async def run(self) -> str:
        try:
            await workflow.execute_activity(
                park_and_count,
                schedule_to_close_timeout=timedelta(seconds=2),
                # Retries available: only the budget, not the count, may stop it.
                retry_policy=RetryPolicy(
                    initial_interval=timedelta(seconds=0.4),
                    backoff_coefficient=1.0,
                    maximum_attempts=100,
                ),
            )
            return "no-timeout"
        except ActivityError as err:
            return _describe(err)


@asynccontextmanager
async def _env() -> AsyncIterator[Client]:
    worker = Worker(
        default_config(),
        task_queue=TASK_QUEUE,
        workflows=[
            OnlyScheduleToCloseHang,
            ScheduleToCloseBeatsStartToClose,
            RetryExhaustsBudget,
            ParkedAsyncScheduleToClose,
        ],
        activities=[hang_forever, always_fails, park_and_count],
    )
    async with worker:
        client = await connect_client()
        try:
            yield client
        finally:
            await client.close()


@pytest.mark.timeout(60)
async def test_schedule_to_close_times_out_hanging_attempt() -> None:
    # Only schedule_to_close set, attempt hangs: must time out, not hang forever.
    async with _env() as client:
        started = _time.monotonic()
        result = await client.execute_workflow(
            OnlyScheduleToCloseHang.run, id="stc-only-hang", task_queue=TASK_QUEUE
        )
        elapsed = _time.monotonic() - started
    assert result == "timeout:SCHEDULE_TO_CLOSE:inner=None:state=TIMEOUT"
    assert elapsed < 30  # bounded by the ~2s budget, not an infinite hang


@pytest.mark.timeout(60)
async def test_parked_async_schedule_to_close_is_terminal_not_retried() -> None:
    # A parked attempt under schedule_to_close must time out terminally and never re-run (regression: the park escaped the budget gate).
    _PARK_INVOCATIONS.clear()
    async with _env() as client:
        started = _time.monotonic()
        result = await client.execute_workflow(
            ParkedAsyncScheduleToClose.run,
            id="stc-parked-async-terminal",
            task_queue=TASK_QUEUE,
        )
        elapsed = _time.monotonic() - started
    assert result == "timeout:SCHEDULE_TO_CLOSE:inner=None:state=TIMEOUT"
    # The discriminating assertion: invoked exactly once (attempt 1), not retried.
    assert _PARK_INVOCATIONS == [1]
    assert elapsed < 30  # bounded by the ~2s budget


@pytest.mark.timeout(60)
async def test_schedule_to_close_cuts_attempt_before_start_to_close() -> None:
    # schedule_to_close (2s) is shorter than start_to_close (60s): the running
    # attempt is cut at the budget with SCHEDULE_TO_CLOSE, not at start_to_close.
    async with _env() as client:
        started = _time.monotonic()
        result = await client.execute_workflow(
            ScheduleToCloseBeatsStartToClose.run,
            id="stc-beats-s2c",
            task_queue=TASK_QUEUE,
        )
        elapsed = _time.monotonic() - started
    assert result == "timeout:SCHEDULE_TO_CLOSE:inner=None:state=TIMEOUT"
    assert elapsed < 30  # cut at ~2s, nowhere near start_to_close's 60s


@pytest.mark.timeout(60)
async def test_schedule_to_close_retry_exhaustion_wraps_last_failure() -> None:
    # Retries stop on the budget: the cause is a SCHEDULE_TO_CLOSE timeout that
    # nests the last application error (matching temporalio).
    async with _env() as client:
        result = await client.execute_workflow(
            RetryExhaustsBudget.run, id="stc-retry-exhaust", task_queue=TASK_QUEUE
        )
    assert result == "timeout:SCHEDULE_TO_CLOSE:inner=ApplicationError:state=TIMEOUT"
