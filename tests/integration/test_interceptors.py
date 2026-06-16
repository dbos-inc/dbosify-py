"""Interceptor tests (DESIGN §6.8): client outbound + activity inbound/outbound.

Both run against an in-process Worker and a Client over the same database, so
the recording interceptor instances (and their event lists) live in the test
process and observe real calls. Activity interceptors are configured on the
Worker; client interceptors on the Client.
"""

import contextvars
from datetime import timedelta
from typing import Any, Dict, List, Optional, Tuple, Type

import pytest
from dbos import DBOSClient

from temporal_dbos import activity, workflow
from temporal_dbos._internal import conversion
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
    QueryRejectCondition,
    RetryPolicy,
    SearchAttributeKey,
    SearchAttributePair,
    TypedSearchAttributes,
)
from temporal_dbos.exceptions import ApplicationError
from temporal_dbos.worker import (
    ActivityInboundInterceptor,
    ActivityOutboundInterceptor,
    ContinueAsNewInput,
    ExecuteActivityInput,
    ExecuteWorkflowInput,
    HandleSignalInput,
)
from temporal_dbos.worker import Interceptor as WorkerInterceptor
from temporal_dbos.worker import (
    StartActivityInput,
    StartChildWorkflowInput,
    Worker,
    WorkflowInboundInterceptor,
    WorkflowInterceptorClassInput,
    WorkflowOutboundInterceptor,
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

    async def describe_workflow(self, input: Any) -> Any:
        self._events.append(("describe_workflow", input.id))
        return await super().describe_workflow(input)

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

    async def update_schedule(self, input: Any) -> None:
        self._events.append(("update_schedule", input.id))
        await super().update_schedule(input)

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


# --- Internal reads must not re-enter the interceptor chain (F1 regression) -


async def test_query_reject_condition_does_not_invoke_describe_interceptor() -> None:
    """A reject-condition query reads workflow status internally; it must not
    fire a describe_workflow interceptor (regression: the relocated _query_impl
    once called the public describe(), which re-entered the chain)."""
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
                InterceptedWorkflow.run, "hi", id="ic-reject", task_queue=TASK_QUEUE
            )
            # RUNNING workflow: NOT_OPEN passes, the query runs.
            assert (
                await handle.query(
                    InterceptedWorkflow.current,
                    reject_condition=QueryRejectCondition.NOT_OPEN,
                )
                == 0
            )
            # An explicit describe() DOES go through the interceptor.
            await handle.describe()
            await handle.signal(InterceptedWorkflow.finish)
            await handle.result()
        finally:
            dbos_client.destroy()

    verbs = [e[0] for e in events]
    assert "query_workflow" in verbs
    # Exactly one describe_workflow event — the explicit describe() — not a
    # second one from the query's internal status read.
    assert verbs.count("describe_workflow") == 1


async def test_schedule_update_does_not_invoke_describe_schedule_interceptor() -> None:
    """ScheduleHandle.update reads the schedule internally; it must not fire a
    describe_schedule interceptor (same F1 regression on the schedule side)."""
    events: List[Event] = []
    worker = Worker(default_config(), task_queue=TASK_QUEUE, workflows=[QuickGreeter])
    async with worker:
        dbos_client = DBOSClient(system_database_url=system_database_url())
        try:
            client = await Client.connect(
                dbos_client, interceptors=[_RecordingClientInterceptor(events)]
            )
            handle = await client.create_schedule(
                "ic-upd", _schedule_for("ic-upd-action")
            )

            def _updater(_: Any) -> None:
                return None  # a no-op update still performs the internal read

            await handle.update(_updater)
            await handle.delete()
        finally:
            dbos_client.destroy()

    verbs = [e[0] for e in events]
    assert "update_schedule" in verbs
    assert "describe_schedule" not in verbs


# --- Workflow interceptors + header propagation (DEVIATIONS D24) -----------
#
# A context-propagation interceptor (the canonical tracing/baggage shape): a
# value set once at the client surfaces in the workflow, its activities, its
# children (and their activities), signal handlers, and across continue-as-new.

HEADER_KEY = "x-trace"

# Process-global but task-local: each workflow run / activity attempt copies its
# own context, so parent / child / activity values never bleed together.
_wf_trace: contextvars.ContextVar[str] = contextvars.ContextVar("wf_trace", default="")
_act_trace: contextvars.ContextVar[str] = contextvars.ContextVar(
    "act_trace", default=""
)


@activity.defn
async def echo_trace() -> str:
    """Returns the trace the activity *interceptor* extracted from headers."""
    return _act_trace.get()


@workflow.defn
class TraceChildWorkflow:
    @workflow.run
    async def run(self) -> Dict[str, str]:
        from_activity = await workflow.execute_activity(
            echo_trace, start_to_close_timeout=timedelta(seconds=30)
        )
        return {"trace": _wf_trace.get(), "activity": from_activity}


@workflow.defn
class TraceParentWorkflow:
    @workflow.run
    async def run(self) -> Dict[str, Any]:
        from_activity = await workflow.execute_activity(
            echo_trace, start_to_close_timeout=timedelta(seconds=30)
        )
        child = await workflow.execute_child_workflow(
            TraceChildWorkflow.run, id="ic-trace-child"
        )
        return {"trace": _wf_trace.get(), "activity": from_activity, "child": child}


@workflow.defn
class TraceSignalWorkflow:
    def __init__(self) -> None:
        self.seen: Optional[str] = None
        self.done = False

    @workflow.signal
    def go(self) -> None:
        # The inbound handle_signal interceptor put the header into _wf_trace.
        self.seen = _wf_trace.get()
        self.done = True

    @workflow.run
    async def run(self) -> str:
        await workflow.wait_condition(lambda: self.done)
        return self.seen or ""


@workflow.defn
class TraceContinueWorkflow:
    @workflow.run
    async def run(self, hops: int) -> str:
        if hops > 0:
            workflow.continue_as_new(args=[hops - 1])
        return _wf_trace.get()


def _with_trace(headers: Dict[str, Any], value: str) -> Dict[str, Any]:
    return {**headers, HEADER_KEY: workflow.payload_converter().to_payload(value)}


class _TraceOutbound(WorkflowOutboundInterceptor):
    def start_activity(self, input: StartActivityInput) -> Any:
        trace = _wf_trace.get()
        if trace:
            input.headers = _with_trace(dict(input.headers), trace)
        return self.next.start_activity(input)

    async def start_child_workflow(self, input: StartChildWorkflowInput) -> Any:
        trace = _wf_trace.get()
        if trace:
            input.headers = _with_trace(dict(input.headers), trace)
        return await self.next.start_child_workflow(input)

    def continue_as_new(self, input: ContinueAsNewInput) -> Any:
        trace = _wf_trace.get()
        if trace:
            input.headers = _with_trace(dict(input.headers), trace)
        self.next.continue_as_new(input)


class _TraceInbound(WorkflowInboundInterceptor):
    def init(self, outbound: WorkflowOutboundInterceptor) -> None:
        super().init(_TraceOutbound(outbound))

    async def execute_workflow(self, input: ExecuteWorkflowInput) -> Any:
        header = input.headers.get(HEADER_KEY)
        if header is not None:
            _wf_trace.set(workflow.payload_converter().from_payload(header))
        return await self.next.execute_workflow(input)

    async def handle_signal(self, input: HandleSignalInput) -> None:
        header = input.headers.get(HEADER_KEY)
        if header is not None:
            _wf_trace.set(workflow.payload_converter().from_payload(header))
        await self.next.handle_signal(input)


class _TraceActivityInbound(ActivityInboundInterceptor):
    async def execute_activity(self, input: ExecuteActivityInput) -> Any:
        header = input.headers.get(HEADER_KEY)
        if header is not None:
            _act_trace.set(activity.payload_converter().from_payload(header))
        return await super().execute_activity(input)


class _TraceWorkerInterceptor(WorkerInterceptor):
    def workflow_interceptor_class(
        self, input: WorkflowInterceptorClassInput
    ) -> Optional[Type[WorkflowInboundInterceptor]]:
        return _TraceInbound

    def intercept_activity(
        self, next: ActivityInboundInterceptor
    ) -> ActivityInboundInterceptor:
        return _TraceActivityInbound(next)


class _TraceClientOutbound(OutboundInterceptor):
    def __init__(self, next: OutboundInterceptor, value: str) -> None:
        super().__init__(next)
        self._value = value

    async def start_workflow(self, input: Any) -> Any:
        pc = conversion.get_converter().payload_converter
        input.headers = {**input.headers, HEADER_KEY: pc.to_payload(self._value)}
        return await super().start_workflow(input)

    async def signal_workflow(self, input: Any) -> None:
        pc = conversion.get_converter().payload_converter
        input.headers = {**input.headers, HEADER_KEY: pc.to_payload(self._value)}
        await super().signal_workflow(input)


class _TraceClientInterceptor(ClientInterceptor):
    def __init__(self, value: str = "hello") -> None:
        self._value = value

    def intercept_client(self, next: OutboundInterceptor) -> OutboundInterceptor:
        return _TraceClientOutbound(next, self._value)


async def test_header_propagates_to_workflow_activity_and_child() -> None:
    """One value set by a client interceptor at start reaches the workflow, its
    activity, the child workflow, and the child's activity."""
    worker = Worker(
        default_config(),
        task_queue=TASK_QUEUE,
        workflows=[TraceParentWorkflow, TraceChildWorkflow],
        activities=[echo_trace],
        interceptors=[_TraceWorkerInterceptor()],
    )
    async with worker:
        dbos_client = DBOSClient(system_database_url=system_database_url())
        try:
            client = await Client.connect(
                dbos_client, interceptors=[_TraceClientInterceptor()]
            )
            result = await client.execute_workflow(
                TraceParentWorkflow.run, id="ic-trace-parent", task_queue=TASK_QUEUE
            )
        finally:
            dbos_client.destroy()

    assert result["trace"] == "hello"
    assert result["activity"] == "hello"
    assert result["child"] == {"trace": "hello", "activity": "hello"}


async def test_header_channel_inert_without_client_injection() -> None:
    """With the workflow interceptor present but no client injection, headers
    are empty everywhere — the prior, header-free behavior is preserved."""
    worker = Worker(
        default_config(),
        task_queue=TASK_QUEUE,
        workflows=[TraceParentWorkflow, TraceChildWorkflow],
        activities=[echo_trace],
        interceptors=[_TraceWorkerInterceptor()],
    )
    async with worker:
        dbos_client = DBOSClient(system_database_url=system_database_url())
        try:
            client = await Client.connect(dbos_client)  # no client interceptor
            result = await client.execute_workflow(
                TraceParentWorkflow.run, id="ic-trace-plain", task_queue=TASK_QUEUE
            )
        finally:
            dbos_client.destroy()

    assert result == {
        "trace": "",
        "activity": "",
        "child": {"trace": "", "activity": ""},
    }


async def test_signal_header_reaches_handler() -> None:
    worker = Worker(
        default_config(),
        task_queue=TASK_QUEUE,
        workflows=[TraceSignalWorkflow],
        activities=[echo_trace],
        interceptors=[_TraceWorkerInterceptor()],
    )
    async with worker:
        dbos_client = DBOSClient(system_database_url=system_database_url())
        try:
            client = await Client.connect(
                dbos_client, interceptors=[_TraceClientInterceptor("from-signal")]
            )
            handle = await client.start_workflow(
                TraceSignalWorkflow.run, id="ic-trace-signal", task_queue=TASK_QUEUE
            )
            await handle.signal(TraceSignalWorkflow.go)
            assert await handle.result() == "from-signal"
        finally:
            dbos_client.destroy()


async def test_header_survives_continue_as_new() -> None:
    """The outbound continue_as_new interceptor re-injects the trace each hop,
    so a value set at start survives the whole chain."""
    worker = Worker(
        default_config(),
        task_queue=TASK_QUEUE,
        workflows=[TraceContinueWorkflow],
        activities=[echo_trace],
        interceptors=[_TraceWorkerInterceptor()],
    )
    async with worker:
        dbos_client = DBOSClient(system_database_url=system_database_url())
        try:
            client = await Client.connect(
                dbos_client, interceptors=[_TraceClientInterceptor()]
            )
            result = await client.execute_workflow(
                TraceContinueWorkflow.run,
                args=[3],
                id="ic-trace-can",
                task_queue=TASK_QUEUE,
            )
        finally:
            dbos_client.destroy()

    assert result == "hello"


async def test_header_survives_payload_codec() -> None:
    """A configured PayloadCodec protects header values and round-trips them
    through the full channel (client -> workflow -> activity -> child)."""
    from typing import Sequence

    from temporal_dbos.converter import DataConverter, Payload, PayloadCodec

    class _XorCodec(PayloadCodec):
        async def encode(self, payloads: Sequence[Payload]) -> List[Payload]:
            return [
                Payload(
                    metadata={**p.metadata, "codec": b"xor"},
                    data=bytes(b ^ 0x5A for b in p.data),
                )
                for p in payloads
            ]

        async def decode(self, payloads: Sequence[Payload]) -> List[Payload]:
            return [
                Payload(
                    metadata={k: v for k, v in p.metadata.items() if k != "codec"},
                    data=bytes(b ^ 0x5A for b in p.data),
                )
                for p in payloads
            ]

    converter = DataConverter(payload_codec=_XorCodec())
    worker = Worker(
        default_config(),
        task_queue=TASK_QUEUE,
        workflows=[TraceParentWorkflow, TraceChildWorkflow],
        activities=[echo_trace],
        interceptors=[_TraceWorkerInterceptor()],
        data_converter=converter,
    )
    async with worker:
        dbos_client = DBOSClient(system_database_url=system_database_url())
        try:
            client = await Client.connect(
                dbos_client,
                interceptors=[_TraceClientInterceptor()],
                data_converter=converter,
            )
            result = await client.execute_workflow(
                TraceParentWorkflow.run, id="ic-trace-codec", task_queue=TASK_QUEUE
            )
        finally:
            dbos_client.destroy()

    # The codec ran on every header hop and the values still arrive intact.
    assert result == {
        "trace": "hello",
        "activity": "hello",
        "child": {"trace": "hello", "activity": "hello"},
    }
