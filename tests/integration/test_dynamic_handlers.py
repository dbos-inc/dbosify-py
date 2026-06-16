"""End-to-end dispatch of dynamic signal/query/update handlers and dynamic
activities (DESIGN §6.1/§6.8), driven through a real Worker + Client. Exact
matches always win over the catch-all; unmatched names/types fall back to it,
and the handler decodes its RawValue args via payload_converter()."""

from contextlib import asynccontextmanager
from datetime import timedelta
from typing import Any, AsyncIterator, Dict, List, Sequence

import pytest
from dbos import DBOSClient

from temporal_dbos import activity, workflow
from temporal_dbos.client import Client
from temporal_dbos.common import RawValue
from temporal_dbos.worker import Worker
from tests.dbconfig import default_config, system_database_url

pytestmark = pytest.mark.usefixtures("tdb_env")

TASK_QUEUE = "dynamic-tq"


@activity.defn
async def known_activity(x: int) -> str:
    return f"known:{x}"


@activity.defn(dynamic=True)
async def any_activity(args: Sequence[RawValue]) -> str:
    pc = activity.payload_converter()
    decoded = [pc.from_payload(a.payload) for a in args]
    return f"{activity.info().activity_type}:{decoded}"


@workflow.defn
class DynamicWorkflow:
    def __init__(self) -> None:
        self.signals: List[List[Any]] = []
        self.done = False

    @workflow.signal
    def explicit(self, x: int) -> None:
        # An exact-match handler must win over the dynamic one below.
        self.signals.append(["EXPLICIT", [x]])

    @workflow.signal(dynamic=True)
    def any_signal(self, name: str, args: Sequence[RawValue]) -> None:
        pc = workflow.payload_converter()
        self.signals.append([name, [pc.from_payload(a.payload) for a in args]])
        if name == "finish":
            self.done = True

    @workflow.query(dynamic=True)
    def any_query(self, name: str, args: Sequence[RawValue]) -> str:
        return f"q:{name}:{len(args)}"

    @workflow.update(dynamic=True)
    def any_update(self, name: str, args: Sequence[RawValue]) -> str:
        pc = workflow.payload_converter()
        decoded = [pc.from_payload(a.payload) for a in args]
        return f"u:{name}:{decoded}"

    @workflow.run
    async def run(self) -> Dict[str, Any]:
        known = await workflow.execute_activity(
            known_activity, args=[7], start_to_close_timeout=timedelta(seconds=10)
        )
        dyn = await workflow.execute_activity(
            "no_such_activity",
            args=["x", 1],
            start_to_close_timeout=timedelta(seconds=10),
        )
        await workflow.wait_condition(lambda: self.done)
        return {"known": known, "dyn": dyn, "signals": self.signals}


@asynccontextmanager
async def _env() -> AsyncIterator[Client]:
    worker = Worker(
        default_config(),
        task_queue=TASK_QUEUE,
        workflows=[DynamicWorkflow],
        activities=[known_activity, any_activity],
    )
    async with worker:
        dbos_client = DBOSClient(system_database_url=system_database_url())
        try:
            yield await Client.connect(dbos_client)
        finally:
            dbos_client.destroy()


async def test_dynamic_handlers_and_activity() -> None:
    async with _env() as client:
        handle = await client.start_workflow(
            DynamicWorkflow.run, id="dyn-wf", task_queue=TASK_QUEUE
        )
        # Unknown query name → dynamic query handler.
        assert await handle.query("status") == "q:status:0"
        # Unknown update name → dynamic update; args decoded via converter.
        assert (
            await handle.execute_update("doThing", args=["a", "b"])
            == "u:doThing:['a', 'b']"
        )
        # Exact-match signal wins over the dynamic handler.
        await handle.signal("explicit", 99)
        # Unknown signal name → dynamic signal.
        await handle.signal("hello", "world")
        await handle.signal("finish")
        result = await handle.result()

    # Exact-match activity routes to its own step; unknown type → dynamic
    # activity, which sees the real requested type as its activity_type.
    assert result["known"] == "known:7"
    assert result["dyn"] == "no_such_activity:['x', 1]"
    assert ["EXPLICIT", [99]] in result["signals"]
    assert ["hello", ["world"]] in result["signals"]
    assert ["finish", []] in result["signals"]
