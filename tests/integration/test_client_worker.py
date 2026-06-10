"""Phase 1 public-API tests: the hello-world quad and friends, written
exactly as a Temporal app would be (imports aside): Client.connect, Worker
in ``async with``, execute_workflow by run-method reference.
"""

from datetime import timedelta
from typing import List, Optional

import pytest

from temporal_dbos import activity, workflow
from temporal_dbos.client import (
    Client,
    WorkflowExecutionStatus,
    WorkflowFailureError,
    WorkflowUpdateFailedError,
)
from temporal_dbos.exceptions import (
    ApplicationError,
    WorkflowAlreadyStartedError,
)
from temporal_dbos.worker import Worker
from tests.dbconfig import system_database_url

pytestmark = pytest.mark.usefixtures("tdb_env")

TASK_QUEUE = "phase1-tq"


@activity.defn
async def compose_greeting(greeting: str, name: str) -> str:
    info = activity.info()
    assert info.activity_type == "compose_greeting"
    assert info.attempt >= 1
    return f"{greeting}, {name}! (wf={info.workflow_id})"


@workflow.defn
class GreetingWorkflow:
    @workflow.run
    async def run(self, name: str) -> str:
        result: str = await workflow.execute_activity(
            compose_greeting,
            args=["Hello", name],
            start_to_close_timeout=timedelta(seconds=10),
        )
        return result


@workflow.defn
class AccumulatorWorkflow:
    def __init__(self) -> None:
        self.total = 0
        self.done = False

    @workflow.signal
    def finish(self) -> None:
        self.done = True

    @workflow.query
    def total_so_far(self) -> int:
        return self.total

    @workflow.update
    def add(self, n: int) -> int:
        self.total += n
        return self.total

    @add.validator
    def _validate_add(self, n: int) -> None:
        if n < 0:
            raise ApplicationError("no negatives", type="BadAmount")

    @workflow.run
    async def run(self) -> int:
        await workflow.wait_condition(lambda: self.done)
        return self.total


@workflow.defn
class FailingWorkflow:
    @workflow.run
    async def run(self) -> None:
        raise ApplicationError("intentional failure", "some-detail", type="MyError")


@workflow.defn
class SignalStartWorkflow:
    def __init__(self) -> None:
        self.greetings: List[str] = []

    @workflow.signal
    def greet(self, name: str) -> None:
        self.greetings.append(name)

    @workflow.run
    async def run(self) -> List[str]:
        await workflow.wait_condition(lambda: len(self.greetings) >= 2)
        return self.greetings


async def _connect() -> Client:
    return await Client.connect(system_database_url())


def _worker(client: Client) -> Worker:
    return Worker(
        client,
        task_queue=TASK_QUEUE,
        workflows=[
            GreetingWorkflow,
            AccumulatorWorkflow,
            FailingWorkflow,
            SignalStartWorkflow,
        ],
        activities=[compose_greeting],
    )


async def test_hello_world_quad() -> None:
    client = await _connect()
    async with _worker(client):
        result = await client.execute_workflow(
            GreetingWorkflow.run, "World", id="hello-wf", task_queue=TASK_QUEUE
        )
    assert result == "Hello, World! (wf=hello-wf)"


async def test_handle_signal_query_update() -> None:
    client = await _connect()
    async with _worker(client):
        handle = await client.start_workflow(
            AccumulatorWorkflow.run, id="acc-wf", task_queue=TASK_QUEUE
        )
        assert await handle.execute_update(AccumulatorWorkflow.add, 5) == 5
        assert await handle.execute_update("add", 3) == 8
        with pytest.raises(WorkflowUpdateFailedError) as exc_info:
            await handle.execute_update(AccumulatorWorkflow.add, -1)
        cause = exc_info.value.__cause__
        assert isinstance(cause, ApplicationError) and cause.type == "BadAmount"
        assert await handle.query(AccumulatorWorkflow.total_so_far) == 8
        await handle.signal(AccumulatorWorkflow.finish)
        assert await handle.result() == 8

        description = await handle.describe()
        assert description.status == WorkflowExecutionStatus.COMPLETED
        assert description.workflow_type == "AccumulatorWorkflow"
        assert description.task_queue == TASK_QUEUE


async def test_workflow_failure_reconstructed() -> None:
    client = await _connect()
    async with _worker(client):
        handle = await client.start_workflow(
            FailingWorkflow.run, id="fail-wf", task_queue=TASK_QUEUE
        )
        with pytest.raises(WorkflowFailureError) as exc_info:
            await handle.result()
        cause = exc_info.value.cause
        assert isinstance(cause, ApplicationError)
        assert cause.type == "MyError"
        assert cause.message == "intentional failure"
        assert list(cause.details) == ["some-detail"]
        assert (await handle.describe()).status == WorkflowExecutionStatus.FAILED


async def test_id_conflict_and_reuse() -> None:
    client = await _connect()
    async with _worker(client):
        handle = await client.start_workflow(
            AccumulatorWorkflow.run, id="reuse-wf", task_queue=TASK_QUEUE
        )
        # Conflict with a RUNNING workflow: Temporal's default behavior fails.
        with pytest.raises(WorkflowAlreadyStartedError):
            await client.start_workflow(
                AccumulatorWorkflow.run, id="reuse-wf", task_queue=TASK_QUEUE
            )
        await handle.signal(AccumulatorWorkflow.finish)
        assert await handle.result() == 0

        # Reuse after close: a new run with a chained run id.
        second = await client.start_workflow(
            AccumulatorWorkflow.run, id="reuse-wf", task_queue=TASK_QUEUE
        )
        assert second.run_id == "reuse-wf--r1"
        await second.signal(AccumulatorWorkflow.finish)
        assert await second.result() == 0

        # An unbound handle resolves to the latest run.
        latest = client.get_workflow_handle("reuse-wf")
        assert (await latest.describe()).run_id == "reuse-wf--r1"


async def test_signal_with_start() -> None:
    client = await _connect()
    async with _worker(client):
        handle = await client.start_workflow(
            SignalStartWorkflow.run,
            id="sws-wf",
            task_queue=TASK_QUEUE,
            start_signal="greet",
            start_signal_args=["first"],
        )
        await handle.signal(SignalStartWorkflow.greet, "second")
        assert await handle.result() == ["first", "second"]


async def test_workflow_id_validation() -> None:
    client = await _connect()
    with pytest.raises(ValueError, match="--r"):
        await client.start_workflow(
            GreetingWorkflow.run, "x", id="bad--r1", task_queue=TASK_QUEUE
        )
