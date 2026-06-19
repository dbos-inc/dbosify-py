"""End-to-end dispatch of dynamic signal/query/update handlers and dynamic
activities (DESIGN §6.1/§6.8), driven through a real Worker + Client. Exact
matches always win over the catch-all; unmatched names/types fall back to it,
and the handler decodes its RawValue args via payload_converter()."""

from contextlib import asynccontextmanager
from datetime import timedelta
from typing import Any, AsyncIterator, Dict, List, Sequence

import pytest
from dbos import DBOSClient

from dbosify import activity, workflow
from dbosify.client import Client
from dbosify.common import RawValue
from dbosify.worker import Worker
from tests.dbconfig import default_config, system_database_url

pytestmark = pytest.mark.usefixtures("dbosify_env")

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


@workflow.defn
class DynamicCarryoverWorkflow:
    """A dynamic-handler analog of test_continue_as_new.CarryoverWorkflow: the
    catch-all handler accumulates by signal name; the run carries that state
    across continue-as-new via its args. Signals sent after the hop are still
    in the inbox when the run closes and must be forwarded (ENCODED) to the new
    run, where the *dynamic* handler decodes them — the interaction this test
    locks in."""

    def __init__(self) -> None:
        self.seen: List[str] = []
        self.hop = False
        self.done = False

    @workflow.signal(dynamic=True)
    def any_signal(self, name: str, args: Sequence[RawValue]) -> None:
        if name == "hop_now":
            self.hop = True
        elif name == "finish":
            self.done = True
        else:
            # The unknown name plus its (forwarded, encoded) arg must both
            # survive — decode the arg via the converter to prove it.
            pc = workflow.payload_converter()
            val = pc.from_payload(args[0].payload) if args else None
            self.seen.append(f"{name}:{val}")

    @workflow.run
    async def run(self, carried: List[str]) -> List[str]:
        self.seen = carried + self.seen
        await workflow.wait_condition(lambda: self.hop or self.done)
        if self.hop:
            workflow.continue_as_new(self.seen)
        return self.seen


@asynccontextmanager
async def _env() -> AsyncIterator[Client]:
    worker = Worker(
        default_config(),
        task_queue=TASK_QUEUE,
        workflows=[DynamicWorkflow, DynamicCarryoverWorkflow],
        activities=[known_activity, any_activity],
    )
    async with worker:
        dbos_client = DBOSClient(system_database_url=system_database_url())
        try:
            yield Client(dbos_client)
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


async def test_dynamic_handler_carryover_across_continue_as_new() -> None:
    """Signals a dynamic handler recorded carry across CAN via the run args,
    and signals still unconsumed at the hop are forwarded (encoded) to the new
    run and decoded by its dynamic handler — order preserved."""
    async with _env() as client:
        handle = await client.start_workflow(
            DynamicCarryoverWorkflow.run, [], id="dyn-carry-wf", task_queue=TASK_QUEUE
        )
        for x in ["d1", "d2", "d3"]:
            await handle.signal(x, x.upper())
        # hop_now triggers CAN; d4/d5 are still in the inbox when the old run
        # closes and must be forwarded to the new run's dynamic handler.
        await handle.signal("hop_now")
        for x in ["d4", "d5"]:
            await handle.signal(x, x.upper())
        await handle.signal("finish")
        assert await handle.result() == [
            "d1:D1",
            "d2:D2",
            "d3:D3",
            "d4:D4",
            "d5:D5",
        ]
