"""How workflow retry policies compose with continue-as-new, cancellation,
and message carryover (Phase 3, DESIGN §6.4). These are the cross-feature
interaction paths — independently each works; the question is the seams.
"""

import asyncio
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

TASK_QUEUE = "retry-interactions-tq"
FAST_RETRY = RetryPolicy(
    initial_interval=timedelta(milliseconds=20), maximum_attempts=5
)


@workflow.defn
class CanThenRetry:
    """Run 0 continues-as-new; the CAN successor fails its first attempt.
    If the CAN run inherits the chain's retry policy (Temporal: yes), it
    retries and attempt 2 succeeds."""

    @workflow.run
    async def run(self, phase: str) -> Dict[str, Any]:
        if phase == "start":
            workflow.continue_as_new("after_can")
        info = workflow.info()
        if info.attempt < 2:
            raise ApplicationError(f"after_can attempt {info.attempt} fails")
        return {
            "attempt": info.attempt,
            "had_failure": workflow.get_last_failure() is not None,
            "continued_run_id": info.continued_run_id,
        }


@workflow.defn
class CancelThenFail:
    """Parks; on cancellation, its cleanup raises a (non-cancel) failure.
    A cancel was requested, so the chain must NOT retry despite the policy
    (Temporal suppresses retry once cancellation is requested)."""

    @workflow.run
    async def run(self) -> str:
        try:
            await workflow.wait_condition(lambda: False)
        except asyncio.CancelledError:
            raise ApplicationError("failed during cancel cleanup")
        return "unreachable"


@workflow.defn
class RetryThenPark:
    """Attempt 1 fails; attempt 2 parks. Lets a test cancel a workflow that
    is on its *second* attempt — the cancel must reach the retry run."""

    @workflow.run
    async def run(self) -> str:
        if workflow.info().attempt == 1:
            raise ApplicationError("attempt 1 fails")
        await workflow.wait_condition(lambda: False)
        return "unreachable"


@asynccontextmanager
async def _env() -> AsyncIterator[Client]:
    worker = Worker(
        default_config(),
        task_queue=TASK_QUEUE,
        workflows=[CanThenRetry, CancelThenFail, RetryThenPark],
        activities=[],
    )
    async with worker:
        dbos_client = DBOSClient(system_database_url=system_database_url())
        try:
            yield await Client.connect(dbos_client)
        finally:
            dbos_client.destroy()


async def _chain_length(client: Client, workflow_id: str) -> int:
    current = await client._current_run(workflow_id)
    assert current is not None
    return current[0] + 1


async def test_can_run_inherits_retry_policy() -> None:
    """A continue-as-new run inherits the chain's retry policy: its failure
    retries and eventually succeeds, instead of ending the chain."""
    async with _env() as client:
        result = await client.execute_workflow(
            CanThenRetry.run,
            "start",
            id="can-retry",
            task_queue=TASK_QUEUE,
            retry_policy=FAST_RETRY,
        )
        assert result["attempt"] == 2
        assert result["had_failure"] is True
        # r0 = CAN, r1 = after_can attempt1 (FAILED), r2 = attempt2 (SUCCESS).
        assert result["continued_run_id"] == "can-retry--r1"
        r0 = await client.get_workflow_handle(
            "can-retry", run_id="can-retry"
        ).describe()
        assert r0.status == WorkflowExecutionStatus.CONTINUED_AS_NEW
        r1 = await client.get_workflow_handle(
            "can-retry", run_id="can-retry--r1"
        ).describe()
        assert r1.status == WorkflowExecutionStatus.FAILED


async def test_cancel_then_fail_suppresses_retry() -> None:
    """A run that fails *after* a cancel was requested does not retry, even
    with a retry policy: cancellation is terminal for the chain."""
    async with _env() as client:
        handle = await client.start_workflow(
            CancelThenFail.run,
            id="cancel-fail",
            task_queue=TASK_QUEUE,
            retry_policy=FAST_RETRY,
        )
        # Park, then cancel; the run's cleanup raises an ApplicationError.
        deadline = asyncio.get_running_loop().time() + 8.0
        while asyncio.get_running_loop().time() < deadline:
            if (await client._status_of("cancel-fail")).status == "PENDING":
                break
            await asyncio.sleep(0.05)
        await handle.cancel()
        with pytest.raises(WorkflowFailureError) as exc_info:
            await handle.result()
        assert "failed during cancel cleanup" in str(exc_info.value.__cause__)
        # No retry: the chain stayed at run 0 despite the policy.
        await asyncio.sleep(0.5)
        assert await _chain_length(client, "cancel-fail") == 1


async def test_cancel_reaches_retry_attempt() -> None:
    """A cooperative cancel addressed to a workflow on its second (retry)
    attempt reaches that run and stops the chain."""
    async with _env() as client:
        handle = await client.start_workflow(
            RetryThenPark.run,
            id="cancel-retry",
            task_queue=TASK_QUEUE,
            retry_policy=FAST_RETRY,
        )
        # Wait until attempt 2 (run --r1) is the parked, running latest.
        deadline = asyncio.get_running_loop().time() + 10.0
        while asyncio.get_running_loop().time() < deadline:
            current = await client._current_run("cancel-retry")
            if (
                current is not None
                and current[0] == 1
                and current[1].status == "PENDING"
            ):
                break
            await asyncio.sleep(0.05)
        assert await _chain_length(client, "cancel-retry") == 2  # on attempt 2
        await handle.cancel()
        with pytest.raises(WorkflowFailureError):
            await handle.result()
        assert (
            await client.get_workflow_handle("cancel-retry").describe()
        ).status == WorkflowExecutionStatus.CANCELED
        # The cancel stopped the chain at the retry attempt (no run 2).
        await asyncio.sleep(0.5)
        assert await _chain_length(client, "cancel-retry") == 2
