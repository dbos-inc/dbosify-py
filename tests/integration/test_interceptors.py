"""Interceptor tests (DESIGN §6.8): client outbound + activity inbound/outbound.

Both run against an in-process Worker and a Client over the same database, so
the recording interceptor instances (and their event lists) live in the test
process and observe real calls. Activity interceptors are configured on the
Worker; client interceptors on the Client.
"""

from datetime import timedelta
from typing import Any, List, Tuple

import pytest
from dbos import DBOSClient

from temporal_dbos import activity, workflow
from temporal_dbos.client import Client
from temporal_dbos.client import Interceptor as ClientInterceptor
from temporal_dbos.client import OutboundInterceptor
from temporal_dbos.worker import (
    ActivityInboundInterceptor,
    ActivityOutboundInterceptor,
    ExecuteActivityInput,
)
from temporal_dbos.worker import Interceptor as WorkerInterceptor
from temporal_dbos.worker import Worker
from tests.dbconfig import default_config, system_database_url

pytestmark = pytest.mark.usefixtures("tdb_env")

TASK_QUEUE = "interceptors-tq"

Event = Tuple[str, Any]


@activity.defn
async def echo(value: str) -> str:
    activity.heartbeat("beat")
    return f"{value}:{activity.info().activity_type}"


@workflow.defn
class InterceptedWorkflow:
    def __init__(self) -> None:
        self.extra = 0
        self.done = False

    @workflow.signal
    def bump(self, n: int) -> None:
        self.extra += n

    @workflow.signal
    def finish(self) -> None:
        self.done = True

    @workflow.query
    def current(self) -> int:
        return self.extra

    @workflow.run
    async def run(self, value: str) -> str:
        out: str = await workflow.execute_activity(
            echo, value, start_to_close_timeout=timedelta(seconds=10)
        )
        await workflow.wait_condition(lambda: self.done)
        return f"{out}|extra={self.extra}"


# --- Activity interceptor -------------------------------------------------


class _RecordingActivityOutbound(ActivityOutboundInterceptor):
    def __init__(self, next: ActivityOutboundInterceptor, events: List[Event]) -> None:
        super().__init__(next)
        self._events = events

    def heartbeat(self, *details: Any) -> None:
        self._events.append(("heartbeat", details))
        super().heartbeat(*details)


class _RecordingActivityInbound(ActivityInboundInterceptor):
    def __init__(self, next: ActivityInboundInterceptor, events: List[Event]) -> None:
        super().__init__(next)
        self._events = events

    def init(self, outbound: ActivityOutboundInterceptor) -> None:
        # Wrap the outbound so info()/heartbeat() route through us too.
        super().init(_RecordingActivityOutbound(outbound, self._events))

    async def execute_activity(self, input: ExecuteActivityInput) -> Any:
        self._events.append(("execute_activity", getattr(input.fn, "__name__", "")))
        result = await super().execute_activity(input)
        # Prove an interceptor can transform the result.
        return f"intercepted({result})"


class _RecordingWorkerInterceptor(WorkerInterceptor):
    def __init__(self, events: List[Event]) -> None:
        self._events = events

    def intercept_activity(
        self, next: ActivityInboundInterceptor
    ) -> ActivityInboundInterceptor:
        return _RecordingActivityInbound(next, self._events)


# --- Client interceptor ---------------------------------------------------


class _RecordingOutbound(OutboundInterceptor):
    def __init__(self, next: OutboundInterceptor, events: List[Event]) -> None:
        super().__init__(next)
        self._events = events

    async def start_workflow(self, input: Any) -> Any:
        self._events.append(("start_workflow", input.id))
        return await super().start_workflow(input)

    async def signal_workflow(self, input: Any) -> None:
        self._events.append(("signal_workflow", input.signal))
        await super().signal_workflow(input)

    async def query_workflow(self, input: Any) -> Any:
        self._events.append(("query_workflow", input.query))
        return await super().query_workflow(input)


class _RecordingClientInterceptor(ClientInterceptor):
    def __init__(self, events: List[Event]) -> None:
        self._events = events

    def intercept_client(self, next: OutboundInterceptor) -> OutboundInterceptor:
        return _RecordingOutbound(next, self._events)


async def test_activity_interceptor_wraps_and_routes() -> None:
    """The activity inbound interceptor wraps execute_activity (and can
    transform the result); the outbound interceptor sees heartbeat()."""
    events: List[Event] = []
    worker = Worker(
        default_config(),
        task_queue=TASK_QUEUE,
        workflows=[InterceptedWorkflow],
        activities=[echo],
        interceptors=[_RecordingWorkerInterceptor(events)],
    )
    async with worker:
        dbos_client = DBOSClient(system_database_url=system_database_url())
        try:
            client = await Client.connect(dbos_client)
            handle = await client.start_workflow(
                InterceptedWorkflow.run, "hi", id="ic-act", task_queue=TASK_QUEUE
            )
            await handle.signal(InterceptedWorkflow.finish)
            result = await handle.result()
        finally:
            dbos_client.destroy()

    # The activity returned "hi:echo"; the inbound interceptor wrapped it.
    assert result == "intercepted(hi:echo)|extra=0"
    kinds = [e[0] for e in events]
    assert "execute_activity" in kinds
    assert "heartbeat" in kinds
    assert ("execute_activity", "echo") in events


async def test_client_interceptor_records_verbs() -> None:
    """Client verbs route through the outbound chain, which sees the resolved
    *Input (workflow id, signal/query names)."""
    events: List[Event] = []
    worker = Worker(
        default_config(),
        task_queue=TASK_QUEUE,
        workflows=[InterceptedWorkflow],
        activities=[echo],
    )
    async with worker:
        dbos_client = DBOSClient(system_database_url=system_database_url())
        try:
            client = await Client.connect(
                dbos_client, interceptors=[_RecordingClientInterceptor(events)]
            )
            handle = await client.start_workflow(
                InterceptedWorkflow.run, "hi", id="ic-client", task_queue=TASK_QUEUE
            )
            await handle.signal(InterceptedWorkflow.bump, 5)
            assert await handle.query(InterceptedWorkflow.current) == 5
            await handle.signal(InterceptedWorkflow.finish)
            assert await handle.result() == "hi:echo|extra=5"
        finally:
            dbos_client.destroy()

    assert ("start_workflow", "ic-client") in events
    assert ("query_workflow", "current") in events
    signalled = [name for kind, name in events if kind == "signal_workflow"]
    assert "bump" in signalled and "finish" in signalled
