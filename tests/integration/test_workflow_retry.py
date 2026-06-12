"""Workflow retry policies (Phase 3, DESIGN §6.4): a failed run starts run
n+1 of the chain with attempt+1 and backoff, `workflow.info().attempt` is
real, attempts see the previous failure via `workflow.get_last_failure()`,
and `result(follow_runs=True)` follows a failed run to its retry successor.
"""

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


async def _chain_length(client: Client, workflow_id: str) -> int:
    """Number of runs in the chain (latest index + 1)."""
    current = await client._current_run(workflow_id)
    assert current is not None
    return current[0] + 1


TASK_QUEUE = "retry-tq"

# Fast backoff so a 3-attempt chain completes in well under a second.
FAST_RETRY = RetryPolicy(
    initial_interval=timedelta(milliseconds=20), maximum_attempts=5
)


@workflow.defn
class SucceedsOnAttempt:
    """Fails until the chain reaches `target` attempts, then reports what it
    observed: the attempt counter and the previous attempt's failure."""

    @workflow.run
    async def run(self, target: int) -> Dict[str, Any]:
        info = workflow.info()
        last = workflow.get_last_failure()
        if info.attempt < target:
            raise ApplicationError(f"failing attempt {info.attempt}")
        return {
            "attempt": info.attempt,
            "run_id": info.run_id,
            "continued_run_id": info.continued_run_id,
            "last_failure": str(last) if last is not None else None,
            "retry_initial": (
                info.retry_policy.initial_interval.total_seconds()
                if info.retry_policy is not None
                else None
            ),
        }


@workflow.defn
class AlwaysFails:
    @workflow.run
    async def run(self, non_retryable: bool) -> None:
        raise ApplicationError(
            f"boom attempt {workflow.info().attempt}", non_retryable=non_retryable
        )


@workflow.defn
class FailsWithType:
    @workflow.run
    async def run(self) -> None:
        raise ApplicationError("typed boom", type="DoNotRetry")


@workflow.defn
class FailsOnceThenReportsTimeout:
    @workflow.run
    async def run(self) -> Dict[str, Any]:
        info = workflow.info()
        if info.attempt == 1:
            raise ApplicationError("attempt 1 fails")
        return {
            "attempt": info.attempt,
            "run_timeout_sec": (
                info.run_timeout.total_seconds()
                if info.run_timeout is not None
                else None
            ),
        }


@asynccontextmanager
async def _env() -> AsyncIterator[Client]:
    worker = Worker(
        default_config(),
        task_queue=TASK_QUEUE,
        workflows=[
            SucceedsOnAttempt,
            AlwaysFails,
            FailsWithType,
            FailsOnceThenReportsTimeout,
        ],
        activities=[],
    )
    async with worker:
        dbos_client = DBOSClient(system_database_url=system_database_url())
        try:
            yield await Client.connect(dbos_client)
        finally:
            dbos_client.destroy()


async def test_retry_until_success() -> None:
    """Attempts 1 and 2 fail; attempt 3 succeeds. execute_workflow (which
    follows runs) returns the success, the chain has dense run ids, the
    failed runs describe as FAILED, and the winning run saw attempt=3 plus
    the previous attempt's failure."""
    async with _env() as client:
        result = await client.execute_workflow(
            SucceedsOnAttempt.run,
            3,
            id="retry-wf",
            task_queue=TASK_QUEUE,
            retry_policy=FAST_RETRY,
        )
        assert result["attempt"] == 3
        assert result["run_id"] == "retry-wf--r2"
        # Retry runs link back to the attempt they retried.
        assert result["continued_run_id"] == "retry-wf--r1"
        assert "failing attempt 2" in result["last_failure"]
        assert result["retry_initial"] == 0.02

        for run_id in ("retry-wf", "retry-wf--r1"):
            description = await client.get_workflow_handle(
                "retry-wf", run_id=run_id
            ).describe()
            assert description.status == WorkflowExecutionStatus.FAILED


async def test_result_follow_runs_false_surfaces_each_attempt() -> None:
    async with _env() as client:
        handle = await client.start_workflow(
            SucceedsOnAttempt.run,
            2,
            id="retry-nofollow",
            task_queue=TASK_QUEUE,
            retry_policy=FAST_RETRY,
        )
        first = client.get_workflow_handle("retry-nofollow", run_id="retry-nofollow")
        with pytest.raises(WorkflowFailureError) as exc_info:
            await first.result(follow_runs=False)
        cause = exc_info.value.__cause__
        assert isinstance(cause, ApplicationError)
        assert "failing attempt 1" in cause.message
        # The unfollowed handle still resolves the chain's eventual success.
        assert (await handle.result())["attempt"] == 2


async def test_retries_exhausted() -> None:
    """maximum_attempts bounds the chain; the final attempt's failure
    surfaces even with follow_runs=True (no successor to follow)."""
    async with _env() as client:
        with pytest.raises(WorkflowFailureError) as exc_info:
            await client.execute_workflow(
                AlwaysFails.run,
                False,
                id="retry-exhaust",
                task_queue=TASK_QUEUE,
                retry_policy=RetryPolicy(
                    initial_interval=timedelta(milliseconds=10), maximum_attempts=2
                ),
            )
        cause = exc_info.value.__cause__
        assert isinstance(cause, ApplicationError)
        assert "boom attempt 2" in cause.message
        # Exactly two runs: the chain stopped at maximum_attempts.
        assert await _chain_length(client, "retry-exhaust") == 2


async def test_no_retry_without_policy() -> None:
    """Workflows do NOT retry by default (Temporal semantics)."""
    async with _env() as client:
        with pytest.raises(WorkflowFailureError) as exc_info:
            await client.execute_workflow(
                AlwaysFails.run, False, id="no-retry", task_queue=TASK_QUEUE
            )
        assert "boom attempt 1" in str(exc_info.value.__cause__)
        assert await _chain_length(client, "no-retry") == 1


async def test_non_retryable_error_stops_immediately() -> None:
    async with _env() as client:
        with pytest.raises(WorkflowFailureError):
            await client.execute_workflow(
                AlwaysFails.run,
                True,  # ApplicationError(non_retryable=True)
                id="non-retryable",
                task_queue=TASK_QUEUE,
                retry_policy=FAST_RETRY,
            )
        assert await _chain_length(client, "non-retryable") == 1


async def test_run_timeout_is_per_attempt() -> None:
    """A retry attempt gets a FRESH run_timeout, assigned when it dequeues.

    Regression: without explicit re-application on the hop, DBOS propagates
    the failed run's *absolute* deadline to the runs it enqueues — here the
    2.5s backoff exceeds the 2s run_timeout, so attempt 2 would be born
    already expired and natively killed at dequeue (surfacing as
    TERMINATED). Temporal applies run_timeout per run.
    """
    async with _env() as client:
        result = await client.execute_workflow(
            FailsOnceThenReportsTimeout.run,
            id="per-attempt-timeout",
            task_queue=TASK_QUEUE,
            run_timeout=timedelta(seconds=2),
            retry_policy=RetryPolicy(
                initial_interval=timedelta(seconds=2.5), maximum_attempts=3
            ),
        )
        assert result == {"attempt": 2, "run_timeout_sec": 2.0}


async def test_non_retryable_error_types() -> None:
    async with _env() as client:
        with pytest.raises(WorkflowFailureError):
            await client.execute_workflow(
                FailsWithType.run,
                id="typed-non-retryable",
                task_queue=TASK_QUEUE,
                retry_policy=RetryPolicy(
                    initial_interval=timedelta(milliseconds=10),
                    non_retryable_error_types=["DoNotRetry"],
                ),
            )
        assert await _chain_length(client, "typed-non-retryable") == 1
