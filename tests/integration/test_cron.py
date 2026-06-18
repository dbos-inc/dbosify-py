"""Legacy cron workflows (Phase 3, DESIGN §6.4): ``start_workflow(
cron_schedule=...)`` creates the first run immediately (delayed to the next
cron occurrence — Temporal's first-task backoff), and each close enqueues
run n+1 of the chain at the next occurrence. Tests use the 6-field
every-second extension so chains advance fast.
"""

import asyncio
import time
from contextlib import asynccontextmanager
from datetime import timedelta
from typing import Any, AsyncIterator, Dict

import pytest
from dbos import DBOSClient

from temporal_dbos import workflow
from temporal_dbos.client import Client, WorkflowExecutionStatus, WorkflowFailureError
from temporal_dbos.common import RetryPolicy
from temporal_dbos.exceptions import ApplicationError
from temporal_dbos.worker import Worker
from tests.dbconfig import default_config, system_database_url

pytestmark = pytest.mark.usefixtures("tdb_env")

TASK_QUEUE = "cron-tq"
EVERY_SECOND = "* * * * * *"


@workflow.defn
class CronCounter:
    """Each run threads a counter through the chain via the last completion
    result and reports what it observed."""

    @workflow.run
    async def run(self) -> Dict[str, Any]:
        info = workflow.info()
        had_last = workflow.has_last_completion_result()
        last = workflow.get_last_completion_result()
        count = (last["count"] + 1) if had_last else 1
        return {
            "count": count,
            "had_last": had_last,
            "cron": info.cron_schedule,
            "attempt": info.attempt,
            "run_id": info.run_id,
            "continued_run_id": info.continued_run_id,
        }


@workflow.defn
class FlakyCron:
    """Fails until a previous failure is visible, then recovers — proving
    cron continues after failures and threads get_last_failure()."""

    @workflow.run
    async def run(self) -> str:
        last = workflow.get_last_failure()
        if last is None:
            raise ApplicationError("first fire fails")
        return f"recovered from: {last}"


@workflow.defn
class ParkingCron:
    """Parks until cancelled (or forever): the cancel is delivered mid-run,
    so the run closes CANCELED and the chain stops."""

    @workflow.run
    async def run(self) -> str:
        await workflow.wait_condition(lambda: False)
        return "unreachable"


@workflow.defn
class CronRetryProbe:
    """Cron + retry_policy together: state is threaded entirely through
    last_completion_result / last_failure / attempt, so each run is
    deterministic and self-describing. Attempt 1 of every cron fire fails;
    attempt 2 succeeds. This exercises retry-within-a-cron-fire (attempt
    increments, the failed attempt is visible), attempt reset on the next
    fire, and simultaneous last_completion + last_failure."""

    @workflow.run
    async def run(self) -> Dict[str, Any]:
        info = workflow.info()
        last_completion = workflow.get_last_completion_result()
        fire = (last_completion["fire"] + 1) if last_completion is not None else 1
        if info.attempt == 1:
            raise ApplicationError(f"fire {fire} attempt 1 fails")
        return {
            "fire": fire,
            "attempt": info.attempt,
            "had_completion": last_completion is not None,
            "had_failure": workflow.get_last_failure() is not None,
            "continued_run_id": info.continued_run_id,
        }


@asynccontextmanager
async def _env() -> AsyncIterator[Client]:
    worker = Worker(
        default_config(),
        task_queue=TASK_QUEUE,
        workflows=[CronCounter, FlakyCron, ParkingCron, CronRetryProbe],
        activities=[],
    )
    async with worker:
        dbos_client = DBOSClient(system_database_url=system_database_url())
        try:
            yield Client(dbos_client)
        finally:
            dbos_client.destroy()


async def _wait_for_chain_index(
    client: Client, workflow_id: str, index: int, timeout: float = 8.0
) -> None:
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        current = await client._current_run(workflow_id)
        if current is not None and current[0] >= index:
            return
        await asyncio.sleep(0.05)
    pytest.fail(f"chain {workflow_id!r} never reached run index {index}")


async def test_cron_chain_fires_and_threads_results() -> None:
    async with _env() as client:
        handle = await client.start_workflow(
            CronCounter.run,
            id="cron-wf",
            task_queue=TASK_QUEUE,
            cron_schedule=EVERY_SECOND,
        )
        # The first run exists immediately (delayed until the first
        # occurrence): describe works before any fire.
        assert (await handle.describe()).status == WorkflowExecutionStatus.RUNNING

        # result() returns the targeted run's result once it fires
        # (documented deviation: temporalio's would follow the cron chain
        # forever).
        first = await handle.result()
        assert first == {
            "count": 1,
            "had_last": False,
            "cron": EVERY_SECOND,
            "attempt": 1,
            "run_id": "cron-wf",
            "continued_run_id": None,
        }

        await _wait_for_chain_index(client, "cron-wf", 2)
        run1 = await client.get_workflow_handle("cron-wf", run_id="cron-wf--r1").result(
            follow_runs=False
        )
        # Run 1 saw run 0's result and links back to it.
        assert run1["count"] == 2
        assert run1["had_last"] is True
        assert run1["continued_run_id"] == "cron-wf"

        # Terminate stops the chain: the live (delayed) run is cancelled
        # natively and no successor appears.
        await client.get_workflow_handle("cron-wf").terminate()
        current = await client._current_run("cron-wf")
        assert current is not None
        stopped_at = current[0]
        await asyncio.sleep(2.2)
        current = await client._current_run("cron-wf")
        assert current is not None and current[0] == stopped_at
        assert (
            await client.get_workflow_handle("cron-wf").describe()
        ).status == WorkflowExecutionStatus.TERMINATED


async def test_cron_continues_after_failure_and_result_follows() -> None:
    async with _env() as client:
        handle = await client.start_workflow(
            FlakyCron.run,
            id="flaky-cron",
            task_queue=TASK_QUEUE,
            cron_schedule=EVERY_SECOND,
        )
        # Run 0 fails; the cron successor sees that failure and recovers.
        # result(follow_runs=True) follows the failed run to it, exactly as
        # temporalio follows new_execution_run_id on a failure event.
        result = await handle.result()
        assert result.startswith("recovered from: first fire fails")

        # Without follow_runs the failure itself surfaces.
        run0 = client.get_workflow_handle("flaky-cron", run_id="flaky-cron")
        with pytest.raises(WorkflowFailureError) as exc_info:
            await run0.result(follow_runs=False)
        assert "first fire fails" in str(exc_info.value.__cause__)
        assert (await run0.describe()).status == WorkflowExecutionStatus.FAILED

        await client.get_workflow_handle("flaky-cron").terminate()


async def test_cron_cancel_of_parked_run_stops_chain() -> None:
    """Cooperative cancel lands in the running (parked) cron run: the run
    closes CANCELED and no successor is enqueued."""
    async with _env() as client:
        handle = await client.start_workflow(
            ParkingCron.run,
            id="parked-cron",
            task_queue=TASK_QUEUE,
            cron_schedule=EVERY_SECOND,
        )
        # Wait for the first fire (the run parks in wait_condition): cancel
        # is only deliverable once the run is consuming its inbox.
        deadline = time.monotonic() + 8.0
        while time.monotonic() < deadline:
            status = await client._status_of("parked-cron")
            if status.status == "PENDING":
                break
            await asyncio.sleep(0.05)
        await handle.cancel()
        with pytest.raises(WorkflowFailureError):
            await handle.result()
        assert (await handle.describe()).status == WorkflowExecutionStatus.CANCELED
        await asyncio.sleep(2.2)
        current = await client._current_run("parked-cron")
        assert current is not None and current[0] == 0


async def test_cron_cancel_of_instant_run_stops_chain() -> None:
    """A cron workflow that never parks can't observe a cooperative cancel
    mid-run — the pending cancel is found at the chain hop instead, ending
    the chain (the closing run stays COMPLETED, matching Temporal's
    suppression of cron continuation once cancellation is requested)."""
    async with _env() as client:
        handle = await client.start_workflow(
            CronCounter.run,
            id="instant-cron",
            task_queue=TASK_QUEUE,
            cron_schedule=EVERY_SECOND,
        )
        await _wait_for_chain_index(client, "instant-cron", 1)
        await handle.cancel()  # lands in the delayed successor's inbox
        await asyncio.sleep(2.5)
        current = await client._current_run("instant-cron")
        assert current is not None
        stopped_at = current[0]
        await asyncio.sleep(2.2)
        current = await client._current_run("instant-cron")
        assert current is not None and current[0] == stopped_at
        assert (
            await client.get_workflow_handle("instant-cron").describe()
        ).status == WorkflowExecutionStatus.COMPLETED


async def test_cron_validation() -> None:
    async with _env() as client:
        with pytest.raises(ValueError, match="Invalid cron schedule"):
            await client.start_workflow(
                CronCounter.run,
                id="bad-cron",
                task_queue=TASK_QUEUE,
                cron_schedule="not a cron",
            )
        with pytest.raises(ValueError, match="start_delay"):
            await client.start_workflow(
                CronCounter.run,
                id="bad-cron-delay",
                task_queue=TASK_QUEUE,
                cron_schedule=EVERY_SECOND,
                start_delay=timedelta(seconds=1),
            )
        # Negative start_delay is rejected (matching temporalio), cron or not.
        with pytest.raises(ValueError, match="non-negative"):
            await client.start_workflow(
                CronCounter.run,
                id="bad-delay",
                task_queue=TASK_QUEUE,
                start_delay=timedelta(seconds=-1),
            )
        # Nothing was created by the rejected starts.
        assert await client._current_run("bad-cron") is None
        assert await client._current_run("bad-cron-delay") is None
        assert await client._current_run("bad-delay") is None


async def test_cron_with_retry_policy() -> None:
    """Cron + retry_policy: attempt 1 of every fire fails and retries to
    attempt 2, which succeeds. Verifies the chain layering — retry hops
    within a fire (attempt increments, the failed attempt is visible via
    get_last_failure), attempt reset to 1 at each new fire, and a run seeing
    last_completion (prior fire's success) and last_failure (this fire's own
    attempt 1) at the same time."""
    async with _env() as client:
        handle = await client.start_workflow(
            CronRetryProbe.run,
            id="cron-retry",
            task_queue=TASK_QUEUE,
            cron_schedule=EVERY_SECOND,
            retry_policy=RetryPolicy(
                initial_interval=timedelta(milliseconds=20), maximum_attempts=2
            ),
        )

        # Chain layout: r0 fire1/attempt1 (FAILED) -> retry -> r1
        # fire1/attempt2 (SUCCESS) -> cron -> r2 fire2/attempt1 (FAILED) ->
        # retry -> r3 fire2/attempt2 (SUCCESS) -> cron -> ...
        await _wait_for_chain_index(client, "cron-retry", 3, timeout=12.0)

        fire1 = await client.get_workflow_handle(
            "cron-retry", run_id="cron-retry--r1"
        ).result(follow_runs=False)
        # First fire: retried to attempt 2, saw its own attempt-1 failure, no
        # prior completion yet. continued_run_id points at the failed attempt.
        assert fire1["fire"] == 1
        assert fire1["attempt"] == 2
        assert fire1["had_completion"] is False
        assert fire1["had_failure"] is True
        assert fire1["continued_run_id"] == "cron-retry"

        fire2 = await client.get_workflow_handle(
            "cron-retry", run_id="cron-retry--r3"
        ).result(follow_runs=False)
        # Second fire: attempt reset to 1 then retried to 2; sees fire 1's
        # completion (carried across the cron hop) AND its own attempt-1
        # failure — the two coexist, as in Temporal.
        assert fire2["fire"] == 2
        assert fire2["attempt"] == 2
        assert fire2["had_completion"] is True
        assert fire2["had_failure"] is True

        # The failed attempts describe as FAILED; the successful ones COMPLETED.
        for run_id, status in [
            ("cron-retry", WorkflowExecutionStatus.FAILED),
            ("cron-retry--r1", WorkflowExecutionStatus.COMPLETED),
            ("cron-retry--r2", WorkflowExecutionStatus.FAILED),
            ("cron-retry--r3", WorkflowExecutionStatus.COMPLETED),
        ]:
            description = await client.get_workflow_handle(
                "cron-retry", run_id=run_id
            ).describe()
            assert description.status == status

        await client.get_workflow_handle("cron-retry").terminate()
