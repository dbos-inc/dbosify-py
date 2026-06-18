"""Runtime handler accessors: workflow.set/get_{signal,query,update}_handler
and the dynamic variants. Covers basic registration, get reflecting state, an
override beating a decorator handler, the buffered-signal flush (a signal that
arrives before its handler is registered is delivered on set), queries, updates
with a validator, and the dynamic catch-all.
"""

import asyncio
from contextlib import asynccontextmanager
from datetime import timedelta
from typing import Any, AsyncIterator, Dict, List, Sequence

import pytest
from dbos import DBOSClient

from temporal_dbos import workflow
from temporal_dbos.client import Client, WorkflowUpdateFailedError
from temporal_dbos.exceptions import ApplicationError
from temporal_dbos.worker import Worker
from tests.dbconfig import default_config, system_database_url

pytestmark = pytest.mark.usefixtures("tdb_env")

TASK_QUEUE = "runtime-handlers-tq"


@workflow.defn
class RuntimeSignalWorkflow:
    def __init__(self) -> None:
        self.received: List[str] = []

    @workflow.run
    async def run(self) -> List[str]:
        workflow.set_signal_handler("dyn", self._on_signal)
        await workflow.wait_condition(lambda: len(self.received) >= 2)
        return self.received

    def _on_signal(self, value: str) -> None:
        self.received.append(value)


@workflow.defn
class GetSetHandlerWorkflow:
    def __init__(self) -> None:
        self.done = False

    @workflow.run
    async def run(self) -> Dict[str, bool]:
        before = workflow.get_signal_handler("foo") is not None
        workflow.set_signal_handler("foo", self._foo)
        after = workflow.get_signal_handler("foo") is not None
        await workflow.wait_condition(lambda: self.done)
        return {"before_set": before, "after_set": after}

    def _foo(self, _: str) -> None:
        self.done = True


@workflow.defn
class OverrideDecoratorWorkflow:
    def __init__(self) -> None:
        self.via = ""
        self.done = False

    @workflow.signal
    def greet(self, who: str) -> None:
        self.via = "decorator"

    @workflow.run
    async def run(self) -> str:
        # Override the @signal handler at runtime; the override must win.
        workflow.set_signal_handler("greet", self._greet_override)
        await workflow.wait_condition(lambda: self.done)
        return self.via

    def _greet_override(self, who: str) -> None:
        self.via = "override"
        self.done = True


@workflow.defn
class BufferedFlushWorkflow:
    """A 'late' signal is sent before its handler exists (buffered). A separate
    decorator signal then triggers registration; setting the handler must
    deliver the buffered signal (temporalio's flush)."""

    def __init__(self) -> None:
        self.ready = False
        self.received: List[str] = []

    @workflow.signal
    def register_now(self) -> None:
        self.ready = True

    @workflow.run
    async def run(self) -> List[str]:
        await workflow.wait_condition(lambda: self.ready)
        workflow.set_signal_handler("late", self._on_late)
        await workflow.wait_condition(lambda: len(self.received) >= 1)
        return self.received

    def _on_late(self, value: str) -> None:
        self.received.append(value)


@workflow.defn
class RuntimeQueryWorkflow:
    def __init__(self) -> None:
        self.done = False

    @workflow.run
    async def run(self) -> None:
        workflow.set_query_handler("ask", self._answer)
        await workflow.wait_condition(lambda: self.done)

    def _answer(self) -> str:
        return "answered"

    @workflow.signal
    def finish(self) -> None:
        self.done = True


@workflow.defn
class RuntimeUpdateWorkflow:
    def __init__(self) -> None:
        self.total = 0
        self.done = False

    @workflow.run
    async def run(self) -> int:
        def validate(v: int) -> None:
            if v < 0:
                raise ApplicationError("negative not allowed")

        def handle(v: int) -> int:
            self.total += v
            return self.total

        workflow.set_update_handler("add", handle, validator=validate)
        await workflow.wait_condition(lambda: self.done)
        return self.total

    @workflow.signal
    def finish(self) -> None:
        self.done = True


@workflow.defn
class DynamicSignalWorkflow:
    def __init__(self) -> None:
        self.names: List[str] = []

    @workflow.run
    async def run(self) -> List[str]:
        workflow.set_dynamic_signal_handler(self._catch_all)
        await workflow.wait_condition(lambda: len(self.names) >= 2)
        return self.names

    def _catch_all(self, name: str, _args: Sequence[Any]) -> None:
        self.names.append(name)


ALL_WORKFLOWS = [
    RuntimeSignalWorkflow,
    GetSetHandlerWorkflow,
    OverrideDecoratorWorkflow,
    BufferedFlushWorkflow,
    RuntimeQueryWorkflow,
    RuntimeUpdateWorkflow,
    DynamicSignalWorkflow,
]


@asynccontextmanager
async def _env() -> AsyncIterator[Client]:
    worker = Worker(default_config(), task_queue=TASK_QUEUE, workflows=ALL_WORKFLOWS)
    async with worker:
        dbos_client = DBOSClient(system_database_url=system_database_url())
        try:
            yield Client(dbos_client)
        finally:
            dbos_client.destroy()


async def test_set_signal_handler_runtime() -> None:
    async with _env() as client:
        handle = await client.start_workflow(
            RuntimeSignalWorkflow.run, id="rt-sig", task_queue=TASK_QUEUE
        )
        await handle.signal("dyn", "a")
        await handle.signal("dyn", "b")
        assert await handle.result() == ["a", "b"]


async def test_get_signal_handler_reflects_registration() -> None:
    async with _env() as client:
        handle = await client.start_workflow(
            GetSetHandlerWorkflow.run, id="rt-getset", task_queue=TASK_QUEUE
        )
        await handle.signal("foo", "x")
        assert await handle.result() == {"before_set": False, "after_set": True}


async def test_override_beats_decorator_handler() -> None:
    async with _env() as client:
        handle = await client.start_workflow(
            OverrideDecoratorWorkflow.run, id="rt-override", task_queue=TASK_QUEUE
        )
        await handle.signal("greet", "world")
        assert await handle.result() == "override"


async def test_set_signal_handler_flushes_buffered_signal() -> None:
    async with _env() as client:
        handle = await client.start_workflow(
            BufferedFlushWorkflow.run, id="rt-flush", task_queue=TASK_QUEUE
        )
        # 'late' arrives with no handler (buffered); register_now then makes
        # run() install the handler, which must deliver the buffered signal.
        await handle.signal("late", "delivered")
        await handle.signal("register_now")
        assert await handle.result() == ["delivered"]


async def test_set_query_handler_runtime() -> None:
    async with _env() as client:
        handle = await client.start_workflow(
            RuntimeQueryWorkflow.run, id="rt-query", task_queue=TASK_QUEUE
        )
        assert await handle.query("ask") == "answered"
        await handle.signal("finish")
        await handle.result()


async def test_set_update_handler_runtime_with_validator() -> None:
    async with _env() as client:
        handle = await client.start_workflow(
            RuntimeUpdateWorkflow.run, id="rt-update", task_queue=TASK_QUEUE
        )
        assert await handle.execute_update("add", 5) == 5
        assert await handle.execute_update("add", 3) == 8
        with pytest.raises(WorkflowUpdateFailedError):
            await handle.execute_update("add", -1)
        await handle.signal("finish")
        assert await handle.result() == 8


async def test_set_dynamic_signal_handler_runtime() -> None:
    async with _env() as client:
        handle = await client.start_workflow(
            DynamicSignalWorkflow.run, id="rt-dyn", task_queue=TASK_QUEUE
        )
        await handle.signal("alpha", 1)
        await handle.signal("beta", 2)
        assert sorted(await handle.result()) == ["alpha", "beta"]
