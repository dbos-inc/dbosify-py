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
from temporal_dbos.client import (
    Client,
)
from temporal_dbos.client import Interceptor as ClientInterceptor
from temporal_dbos.client import (
    OutboundInterceptor,
    Schedule,
    ScheduleActionStartWorkflow,
    ScheduleIntervalSpec,
    ScheduleSpec,
)
from temporal_dbos.common import (
    RetryPolicy,
    SearchAttributeKey,
    SearchAttributePair,
    TypedSearchAttributes,
)
from temporal_dbos.exceptions import ApplicationError
from temporal_dbos.worker import (
    ActivityInboundInterceptor,
    ActivityOutboundInterceptor,
    ExecuteActivityInput,
)
from temporal_dbos.worker import Interceptor as WorkerInterceptor
from temporal_dbos.worker import (
    Worker,
)
from tests.dbconfig import default_config, system_database_url
from tests.harness import retry_until_success_async

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

    async def create_schedule(self, input: Any) -> Any:
        self._events.append(("create_schedule", input.id))
        return await super().create_schedule(input)

    async def describe_schedule(self, input: Any) -> Any:
        self._events.append(("describe_schedule", input.id))
        return await super().describe_schedule(input)

    async def pause_schedule(self, input: Any) -> None:
        self._events.append(("pause_schedule", input.id))
        await super().pause_schedule(input)

    async def delete_schedule(self, input: Any) -> None:
        self._events.append(("delete_schedule", input.id))
        await super().delete_schedule(input)

    async def heartbeat_async_activity(self, input: Any) -> None:
        self._events.append(("heartbeat_async_activity", input.details))
        await super().heartbeat_async_activity(input)

    async def complete_async_activity(self, input: Any) -> None:
        self._events.append(("complete_async_activity", input.result))
        await super().complete_async_activity(input)


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


# --- Schedule + async-activity verbs --------------------------------------


@workflow.defn
class QuickGreeter:
    @workflow.run
    async def run(self, name: str) -> str:
        return f"hi {name}"


def _schedule_for(action_id: str) -> Schedule:
    return Schedule(
        action=ScheduleActionStartWorkflow(
            QuickGreeter.run, "x", id=action_id, task_queue=TASK_QUEUE
        ),
        spec=ScheduleSpec(intervals=[ScheduleIntervalSpec(every=timedelta(hours=1))]),
    )


TOKENS: List[bytes] = []


@activity.defn
async def complete_externally() -> str:
    TOKENS.append(activity.info().task_token)
    activity.raise_complete_async()


@workflow.defn
class AsyncCompleteWorkflow:
    @workflow.run
    async def run(self) -> str:
        result: str = await workflow.execute_activity(
            complete_externally, start_to_close_timeout=timedelta(seconds=60)
        )
        return result


async def test_client_interceptor_records_schedule_verbs() -> None:
    events: List[Event] = []
    worker = Worker(default_config(), task_queue=TASK_QUEUE, workflows=[QuickGreeter])
    async with worker:
        dbos_client = DBOSClient(system_database_url=system_database_url())
        try:
            client = await Client.connect(
                dbos_client, interceptors=[_RecordingClientInterceptor(events)]
            )
            handle = await client.create_schedule(
                "ic-sched", _schedule_for("ic-sched-action")
            )
            await handle.describe()
            await handle.pause(note="paused")
            await handle.delete()
        finally:
            dbos_client.destroy()

    verbs = [e[0] for e in events]
    for verb in (
        "create_schedule",
        "describe_schedule",
        "pause_schedule",
        "delete_schedule",
    ):
        assert verb in verbs, verb
    assert ("create_schedule", "ic-sched") in events


async def test_client_interceptor_records_async_activity_verbs() -> None:
    events: List[Event] = []
    TOKENS.clear()
    worker = Worker(
        default_config(),
        task_queue=TASK_QUEUE,
        workflows=[AsyncCompleteWorkflow],
        activities=[complete_externally],
    )
    async with worker:
        dbos_client = DBOSClient(system_database_url=system_database_url())
        try:
            client = await Client.connect(
                dbos_client, interceptors=[_RecordingClientInterceptor(events)]
            )
            handle = await client.start_workflow(
                AsyncCompleteWorkflow.run, id="ic-async", task_queue=TASK_QUEUE
            )

            async def _token() -> bytes:
                assert TOKENS, "activity has not parked yet"
                return TOKENS[0]

            token = await retry_until_success_async(_token)
            async_handle = client.get_async_activity_handle(task_token=token)
            await async_handle.heartbeat("progress")
            await async_handle.complete("done-externally")
            assert await handle.result() == "done-externally"
        finally:
            dbos_client.destroy()

    verbs = [e[0] for e in events]
    assert "heartbeat_async_activity" in verbs
    assert "complete_async_activity" in verbs


# --- Multi-interceptor ordering -------------------------------------------


class _OrderOutbound(OutboundInterceptor):
    def __init__(self, next: OutboundInterceptor, tag: str, order: List[str]) -> None:
        super().__init__(next)
        self._tag = tag
        self._order = order

    async def start_workflow(self, input: Any) -> Any:
        self._order.append(self._tag)
        return await super().start_workflow(input)


class _OrderClientInterceptor(ClientInterceptor):
    def __init__(self, tag: str, order: List[str]) -> None:
        self._tag = tag
        self._order = order

    def intercept_client(self, next: OutboundInterceptor) -> OutboundInterceptor:
        return _OrderOutbound(next, self._tag, self._order)


class _OrderInbound(ActivityInboundInterceptor):
    def __init__(
        self, next: ActivityInboundInterceptor, tag: str, order: List[str]
    ) -> None:
        super().__init__(next)
        self._tag = tag
        self._order = order

    async def execute_activity(self, input: ExecuteActivityInput) -> Any:
        self._order.append(self._tag)
        return await super().execute_activity(input)


class _OrderWorkerInterceptor(WorkerInterceptor):
    def __init__(self, tag: str, order: List[str]) -> None:
        self._tag = tag
        self._order = order

    def intercept_activity(
        self, next: ActivityInboundInterceptor
    ) -> ActivityInboundInterceptor:
        return _OrderInbound(next, self._tag, self._order)


@workflow.defn
class LocalActivityWorkflow:
    @workflow.run
    async def run(self, value: str) -> str:
        result: str = await workflow.execute_local_activity(
            echo, value, start_to_close_timeout=timedelta(seconds=10)
        )
        return result


async def test_interceptor_chain_ordering() -> None:
    """The first interceptor in the list is outermost and runs first, on both
    the client and activity chains (temporalio's reversed() fold)."""
    client_order: List[str] = []
    activity_order: List[str] = []
    worker = Worker(
        default_config(),
        task_queue=TASK_QUEUE,
        workflows=[LocalActivityWorkflow],
        activities=[echo],
        interceptors=[
            _OrderWorkerInterceptor("A", activity_order),
            _OrderWorkerInterceptor("B", activity_order),
        ],
    )
    async with worker:
        dbos_client = DBOSClient(system_database_url=system_database_url())
        try:
            client = await Client.connect(
                dbos_client,
                interceptors=[
                    _OrderClientInterceptor("A", client_order),
                    _OrderClientInterceptor("B", client_order),
                ],
            )
            await client.execute_workflow(
                LocalActivityWorkflow.run, "x", id="ic-order", task_queue=TASK_QUEUE
            )
        finally:
            dbos_client.destroy()

    assert client_order == ["A", "B"]
    assert activity_order == ["A", "B"]


# --- Per attempt, not on replay -------------------------------------------


_attempts = {"fn": 0, "exec": 0}


@activity.defn
async def flaky() -> str:
    _attempts["fn"] += 1
    if _attempts["fn"] == 1:
        raise ApplicationError("retry me", type="Retry")
    return "ok"


class _CountingInbound(ActivityInboundInterceptor):
    async def execute_activity(self, input: ExecuteActivityInput) -> Any:
        _attempts["exec"] += 1
        return await super().execute_activity(input)


class _CountingWorkerInterceptor(WorkerInterceptor):
    def intercept_activity(
        self, next: ActivityInboundInterceptor
    ) -> ActivityInboundInterceptor:
        return _CountingInbound(next)


@workflow.defn
class RetryReplayWorkflow:
    def __init__(self) -> None:
        self.bumps = 0
        self.done = False

    @workflow.signal
    def bump(self) -> None:
        self.bumps += 1

    @workflow.signal
    def finish(self) -> None:
        self.done = True

    @workflow.run
    async def run(self) -> str:
        r: str = await workflow.execute_activity(
            flaky,
            start_to_close_timeout=timedelta(seconds=10),
            retry_policy=RetryPolicy(
                initial_interval=timedelta(milliseconds=1), maximum_attempts=3
            ),
        )
        await workflow.wait_condition(lambda: self.done)
        return f"{r}:{self.bumps}"


async def test_activity_interceptor_fires_per_attempt_not_on_replay() -> None:
    _attempts["fn"] = 0
    _attempts["exec"] = 0
    worker = Worker(
        default_config(),
        task_queue=TASK_QUEUE,
        workflows=[RetryReplayWorkflow],
        activities=[flaky],
        interceptors=[_CountingWorkerInterceptor()],
    )
    async with worker:
        dbos_client = DBOSClient(system_database_url=system_database_url())
        try:
            client = await Client.connect(dbos_client)
            handle = await client.start_workflow(
                RetryReplayWorkflow.run, id="ic-retry", task_queue=TASK_QUEUE
            )
            # Each signal drives a workflow task that replays run() from the
            # top; the activity step result is reused from its checkpoint.
            for _ in range(3):
                await handle.signal(RetryReplayWorkflow.bump)
            await handle.signal(RetryReplayWorkflow.finish)
            assert await handle.result() == "ok:3"
        finally:
            dbos_client.destroy()

    # One failed attempt + one success; the replays do not re-run the step.
    assert _attempts["fn"] == 2
    assert _attempts["exec"] == 2


# --- Local activities ------------------------------------------------------


async def test_activity_interceptor_fires_for_local_activity() -> None:
    events: List[Event] = []
    worker = Worker(
        default_config(),
        task_queue=TASK_QUEUE,
        workflows=[LocalActivityWorkflow],
        activities=[echo],
        interceptors=[_RecordingWorkerInterceptor(events)],
    )
    async with worker:
        dbos_client = DBOSClient(system_database_url=system_database_url())
        try:
            client = await Client.connect(dbos_client)
            result = await client.execute_workflow(
                LocalActivityWorkflow.run, "loc", id="ic-local", task_queue=TASK_QUEUE
            )
        finally:
            dbos_client.destroy()

    assert result == "intercepted(loc:echo)"
    assert ("execute_activity", "echo") in events


# --- Interceptor x search attributes (the post-merge seam) ----------------


INJECTED_SA = SearchAttributeKey.for_keyword("InjectedByInterceptor")


class _InjectingOutbound(OutboundInterceptor):
    """Injects a search attribute + memo entry onto every workflow start —
    the canonical context-propagation use case."""

    async def start_workflow(self, input: Any) -> Any:
        input.search_attributes = TypedSearchAttributes(
            [SearchAttributePair(INJECTED_SA, "yes")]
        )
        memo = dict(input.memo) if input.memo else {}
        memo["injected_memo"] = "from-interceptor"
        input.memo = memo
        return await super().start_workflow(input)


class _InjectingClientInterceptor(ClientInterceptor):
    def intercept_client(self, next: OutboundInterceptor) -> OutboundInterceptor:
        return _InjectingOutbound(next)


async def test_client_interceptor_injects_search_attributes_and_memo() -> None:
    """A client interceptor that mutates StartWorkflowInput.search_attributes /
    .memo is honored durably: the injected values flow through the relocated
    _start_workflow_impl into the attributes column and back out via describe().
    Guards the seam where interceptors meet search attributes."""
    worker = Worker(default_config(), task_queue=TASK_QUEUE, workflows=[QuickGreeter])
    async with worker:
        dbos_client = DBOSClient(system_database_url=system_database_url())
        try:
            client = await Client.connect(
                dbos_client, interceptors=[_InjectingClientInterceptor()]
            )
            # Started with neither search_attributes nor memo — the interceptor
            # adds both.
            handle = await client.start_workflow(
                QuickGreeter.run, "x", id="ic-inject", task_queue=TASK_QUEUE
            )
            assert await handle.result() == "hi x"

            desc = await handle.describe()
            assert desc.typed_search_attributes[INJECTED_SA] == "yes"
            assert desc.search_attributes["InjectedByInterceptor"] == ["yes"]
            assert await desc.memo() == {"injected_memo": "from-interceptor"}
        finally:
            dbos_client.destroy()
