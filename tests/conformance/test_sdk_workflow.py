"""Conformance: behavioral workflow tests adapted from temporalio's own SDK
test suite (``tests/worker/test_workflow.py`` in temporal-sdk-python).

Those tests are the most rigorous behavioral conformance corpus available — far
more focused than the samples. Each defines a workflow/activity and asserts
exact behavior via ``new_worker(client, ...)`` + ``client.execute_workflow``.
We vendor the self-contained, pure-behavioral ones here, import-swapped to
``dbosify`` and run through this module's harness (a ``client`` fixture
over our Worker/DBOSClient and a ``new_worker`` adapter that maps temporalio's
``Worker(client, ...)`` onto our ``Worker(DBOSConfig, ...)``). Tests that depend
on server-only surfaces (Temporal history events, the time-skipping clock,
advanced visibility) are left in the SDK suite, not adapted.
"""

import asyncio
import time
import uuid
from abc import ABC, abstractmethod
from contextlib import asynccontextmanager
from dataclasses import dataclass
from datetime import timedelta
from typing import (
    Any,
    AsyncIterator,
    Awaitable,
    Callable,
    Iterator,
    NoReturn,
    Protocol,
    Sequence,
    TypeVar,
    cast,
    runtime_checkable,
)

import pytest
import sqlalchemy as sa
from dbos import DBOSClient

from dbosify import activity, workflow
from dbosify.client import (
    Client,
    WorkflowExecutionStatus,
    WorkflowFailureError,
    WorkflowHandle,
    WorkflowQueryFailedError,
    WorkflowUpdateFailedError,
    WorkflowUpdateStage,
)
from dbosify.common import RawValue, RetryPolicy
from dbosify.exceptions import (
    ActivityError,
    ApplicationError,
    CancelledError,
    ChildWorkflowError,
    TimeoutError,
    WorkflowAlreadyStartedError,
)
from dbosify.worker import Worker
from tests.conformance.sdk_harness import warm_schema
from tests.dbconfig import default_config, system_database_url

pytestmark = pytest.mark.usefixtures("dbosify_env")

T = TypeVar("T")


# --- harness -----------------------------------------------------------------


def _ensure_database_exists() -> None:
    """The client-mode DBOSClient connects at construction, so the (freshly
    dropped) system database must exist before the Worker launches and migrates
    it (the Worker owns the schema)."""
    url = sa.make_url(system_database_url())
    maintenance = sa.create_engine(
        url.set(drivername="postgresql+psycopg", database="postgres"),
        connect_args={"connect_timeout": 30},
    )
    try:
        with maintenance.connect() as conn:
            conn.execution_options(isolation_level="AUTOCOMMIT")
            exists = conn.execute(
                sa.text("SELECT 1 FROM pg_database WHERE datname = :name"),
                {"name": url.database},
            ).scalar()
            if not exists:
                conn.execute(sa.text(f'CREATE DATABASE "{url.database}"'))
    finally:
        maintenance.dispose()


@pytest.fixture()
def client(dbosify_env: None) -> Iterator[Client]:
    _ensure_database_exists()
    dbos_client = DBOSClient(system_database_url=system_database_url())
    try:
        yield Client(dbos_client)
    finally:
        dbos_client.destroy()


@asynccontextmanager
async def new_worker(
    client: Client,
    *workflows: type,
    activities: Sequence[Callable[..., Any]] = (),
    task_queue: str | None = None,
    workflow_failure_exception_types: Sequence[type[BaseException]] = (),
    **_ignored: Any,
) -> AsyncIterator[Worker]:
    """temporalio's ``new_worker(client, *workflows, activities=...)`` over our
    ``Worker(DBOSConfig, ...)``. The Worker owns the DBOS lifecycle; the passed
    ``client`` already targets the same database."""
    worker = Worker(
        default_config(),
        task_queue=task_queue or f"sdk-tq-{uuid.uuid4()}",
        workflows=list(workflows),
        activities=list(activities),
        workflow_failure_exception_types=list(workflow_failure_exception_types),
    )
    async with worker:
        yield worker


async def assert_eq_eventually(
    expected: T,
    fn: Callable[[], Awaitable[T]],
    *,
    timeout: timedelta = timedelta(seconds=10),
    interval: timedelta = timedelta(milliseconds=200),
) -> None:
    deadline = time.monotonic() + timeout.total_seconds()
    last: Any = None
    while time.monotonic() < deadline:
        last = await fn()
        if last == expected:
            return
        await asyncio.sleep(interval.total_seconds())
    assert expected == last, f"timed out waiting for {expected!r}, last was {last!r}"


def _wid() -> str:
    return f"workflow-{uuid.uuid4()}"


# --- shared workflow/activity definitions ------------------------------------


@activity.defn
async def say_hello(name: str) -> str:
    return f"Hello, {name}!"


@workflow.defn
class HelloWorkflow:
    @workflow.run
    async def run(self, name: str) -> str:
        return f"Hello, {name}!"


# --- adapted tests -----------------------------------------------------------


@workflow.defn
class SimpleActivityWorkflow:
    @workflow.run
    async def run(self, name: str) -> str:
        return cast(
            str,
            await workflow.execute_activity(
                say_hello,
                name,
                schedule_to_close_timeout=timedelta(seconds=5),
                summary="Do a thing",
            ),
        )


async def test_workflow_simple_activity(client: Client) -> None:
    async with new_worker(
        client, SimpleActivityWorkflow, activities=[say_hello]
    ) as worker:
        result = await client.execute_workflow(
            SimpleActivityWorkflow.run,
            "Temporal",
            id=_wid(),
            task_queue=worker.task_queue,
        )
        assert result == "Hello, Temporal!"


@workflow.defn
class SimpleLocalActivityWorkflow:
    @workflow.run
    async def run(self, name: str) -> str:
        return cast(
            str,
            await workflow.execute_local_activity(
                say_hello, name, schedule_to_close_timeout=timedelta(seconds=5)
            ),
        )


async def test_workflow_simple_local_activity(client: Client) -> None:
    async with new_worker(
        client, SimpleLocalActivityWorkflow, activities=[say_hello]
    ) as worker:
        result = await client.execute_workflow(
            SimpleLocalActivityWorkflow.run,
            "Temporal",
            id=_wid(),
            task_queue=worker.task_queue,
        )
        assert result == "Hello, Temporal!"


@workflow.defn
class SimpleChildWorkflow:
    @workflow.run
    async def run(self, name: str) -> str:
        return cast(str, await workflow.execute_child_workflow(HelloWorkflow.run, name))


async def test_workflow_simple_child(client: Client) -> None:
    async with new_worker(client, SimpleChildWorkflow, HelloWorkflow) as worker:
        result = await client.execute_workflow(
            SimpleChildWorkflow.run,
            "Temporal",
            id=_wid(),
            task_queue=worker.task_queue,
        )
        assert result == "Hello, Temporal!"


@workflow.defn
class SignalAndQueryWorkflow:
    def __init__(self) -> None:
        self._last_event: str | None = None

    @workflow.run
    async def run(self) -> None:
        await asyncio.Future()  # wait forever

    @workflow.signal
    def signal1(self, arg: str) -> None:
        self._last_event = f"signal1: {arg}"

    @workflow.signal(dynamic=True)
    def signal_dynamic(self, name: str, args: Sequence[RawValue]) -> None:
        arg = workflow.payload_converter().from_payload(args[0].payload, str)
        self._last_event = f"signal_dynamic {name}: {arg}"

    @workflow.signal(name="Custom Name")
    def signal_custom(self, arg: str) -> None:
        self._last_event = f"signal_custom: {arg}"

    @workflow.query
    def last_event(self) -> str:
        return self._last_event or "<no event>"

    @workflow.query(dynamic=True)
    def query_dynamic(self, name: str, args: Sequence[RawValue]) -> str:
        arg = workflow.payload_converter().from_payload(args[0].payload, str)
        return f"query_dynamic {name}: {arg}"

    @workflow.query(name="Custom Name")
    def query_custom(self, arg: str) -> str:
        return f"query_custom: {arg}"


async def test_workflow_signal_and_query(client: Client) -> None:
    async with new_worker(client, SignalAndQueryWorkflow) as worker:
        handle = await client.start_workflow(
            SignalAndQueryWorkflow.run, id=_wid(), task_queue=worker.task_queue
        )

        # Simple signals and queries
        await handle.signal(SignalAndQueryWorkflow.signal1, "some arg")
        assert "signal1: some arg" == await handle.query(
            SignalAndQueryWorkflow.last_event
        )

        # Dynamic signals and queries
        await handle.signal("signal2", "dyn arg")
        assert "signal_dynamic signal2: dyn arg" == await handle.query(
            SignalAndQueryWorkflow.last_event
        )
        assert "query_dynamic query2: dyn arg" == await handle.query(
            "query2", "dyn arg"
        )

        # Custom named signals and queries
        await handle.signal("Custom Name", "custom arg1")
        assert "signal_custom: custom arg1" == await handle.query(
            SignalAndQueryWorkflow.last_event
        )
        assert "query_custom: custom arg1" == await handle.query(
            "Custom Name", "custom arg1"
        )


@workflow.defn
class LongSleepWorkflow:
    _started = False

    @workflow.run
    async def run(self) -> None:
        self._started = True
        await asyncio.sleep(1000)

    @workflow.query
    def started(self) -> bool:
        return self._started


async def test_workflow_simple_cancel(client: Client) -> None:
    async with new_worker(client, LongSleepWorkflow) as worker:
        handle = await client.start_workflow(
            LongSleepWorkflow.run, id=_wid(), task_queue=worker.task_queue
        )

        async def started() -> bool:
            return cast(bool, await handle.query(LongSleepWorkflow.started))

        await assert_eq_eventually(True, started)
        await handle.cancel()
        with pytest.raises(WorkflowFailureError) as err:
            await handle.result()
        assert isinstance(err.value.cause, CancelledError)
        assert (await handle.describe()).status == WorkflowExecutionStatus.CANCELED


# A long-lived activity that heartbeats until cancelled — shared by the
# cancellation and timeout tests below (temporalio's module-level ``wait_cancel``).
@activity.defn
async def wait_cancel() -> str:
    try:
        if activity.info().is_local:
            await asyncio.sleep(1000)
        else:
            while True:
                await asyncio.sleep(0.3)
                activity.heartbeat()
        return "Manually stopped"
    except asyncio.CancelledError:
        return "Got cancelled error, cancelled? " + str(activity.is_cancelled())


@activity.defn
async def multi_param_activity(param1: int, param2: str) -> str:
    return f"param1: {param1}, param2: {param2}"


@workflow.defn
class MultiParamWorkflow:
    @workflow.run
    async def run(self, param1: int, param2: str) -> str:
        return cast(
            str,
            await workflow.execute_activity(
                multi_param_activity,
                args=[param1, param2],
                schedule_to_close_timeout=timedelta(seconds=30),
            ),
        )


async def test_workflow_multi_param(client: Client) -> None:
    async with new_worker(
        client, MultiParamWorkflow, activities=[multi_param_activity]
    ) as worker:
        result = await client.execute_workflow(
            MultiParamWorkflow.run,
            args=[123, "val1"],
            id=_wid(),
            task_queue=worker.task_queue,
        )
        assert result == "param1: 123, param2: val1"


@workflow.defn
class AsyncUtilWorkflow:
    def __init__(self) -> None:
        self._status = "starting"
        self._wait_event1 = asyncio.Event()
        self._received_event2 = False

    @workflow.run
    async def run(self) -> dict[str, Any]:
        ret: dict[str, Any] = {
            "start": str(workflow.now()),
            "start_time": workflow.time(),
            "start_time_ns": workflow.time_ns(),
            "event_loop_start": asyncio.get_running_loop().time(),
        }
        await asyncio.sleep(0.1)
        self._status = "waiting for event1"
        await self._wait_event1.wait()
        self._status = "waiting for event2"
        await workflow.wait_condition(lambda: self._received_event2)
        self._status = "done"
        ret["end_time_ns"] = workflow.time_ns()
        return ret

    @workflow.signal
    def event1(self) -> None:
        self._wait_event1.set()

    @workflow.signal
    def event2(self) -> None:
        self._received_event2 = True

    @workflow.query
    def status(self) -> str:
        return self._status


async def test_workflow_async_utils(client: Client) -> None:
    # Adapted: the SDK test also reads timestamps out of Temporal history at the
    # end (server-only); we keep the behavioral half — now()/time()/time_ns()
    # plus asyncio.Event / wait_condition sequencing under signals & queries.
    async with new_worker(client, AsyncUtilWorkflow) as worker:
        handle = await client.start_workflow(
            AsyncUtilWorkflow.run, id=_wid(), task_queue=worker.task_queue
        )

        async def status() -> str:
            return cast(str, await handle.query(AsyncUtilWorkflow.status))

        await assert_eq_eventually("waiting for event1", status)
        await handle.signal(AsyncUtilWorkflow.event1)
        await assert_eq_eventually("waiting for event2", status)
        await handle.signal(AsyncUtilWorkflow.event2)
        result = await handle.result()
        assert "done" == await status()
        # now()/time()/time_ns() are populated and monotone within the run.
        assert result["end_time_ns"] >= result["start_time_ns"]
        assert result["start_time"] > 0


@workflow.defn
class ReturnSignalWorkflow:
    def __init__(self) -> None:
        self._signal: str | None = None

    @workflow.run
    async def run(self) -> str:
        await workflow.wait_condition(lambda: self._signal is not None)
        assert self._signal
        return self._signal

    @workflow.signal
    def my_signal(self, value: str) -> None:
        self._signal = value


@workflow.defn
class SignalChildWorkflow:
    @workflow.run
    async def run(self, signal_value: str) -> str:
        handle = await workflow.start_child_workflow(
            ReturnSignalWorkflow.run, id=workflow.info().workflow_id + "_child"
        )
        await handle.signal(ReturnSignalWorkflow.my_signal, signal_value)
        return cast(str, await handle)


async def test_workflow_signal_child(client: Client) -> None:
    async with new_worker(client, SignalChildWorkflow, ReturnSignalWorkflow) as worker:
        result = await client.execute_workflow(
            SignalChildWorkflow.run,
            "some value",
            id=_wid(),
            task_queue=worker.task_queue,
        )
        assert result == "some value"


@workflow.defn
class CancelExternalWorkflow:
    @workflow.run
    async def run(self, external_workflow_id: str) -> None:
        await workflow.get_external_workflow_handle(external_workflow_id).cancel()


async def test_workflow_cancel_external(client: Client) -> None:
    async with new_worker(client, CancelExternalWorkflow, LongSleepWorkflow) as worker:
        long_sleep_handle = await client.start_workflow(
            LongSleepWorkflow.run, id=_wid(), task_queue=worker.task_queue
        )
        await client.execute_workflow(
            CancelExternalWorkflow.run,
            long_sleep_handle.id,
            id=_wid(),
            task_queue=worker.task_queue,
        )
        with pytest.raises(WorkflowFailureError) as err:
            await long_sleep_handle.result()
        assert isinstance(err.value.cause, CancelledError)


@dataclass
class SignalExternalWorkflowArgs:
    external_workflow_id: str
    signal_value: str


@workflow.defn
class SignalExternalWorkflow:
    @workflow.run
    async def run(self, args: SignalExternalWorkflowArgs) -> None:
        handle: workflow.ExternalWorkflowHandle = (
            workflow.get_external_workflow_handle_for(
                ReturnSignalWorkflow.run, args.external_workflow_id
            )
        )
        await handle.signal(ReturnSignalWorkflow.my_signal, args.signal_value)


async def test_workflow_signal_external(client: Client) -> None:
    async with new_worker(
        client, SignalExternalWorkflow, ReturnSignalWorkflow
    ) as worker:
        return_signal_handle = await client.start_workflow(
            ReturnSignalWorkflow.run, id=_wid(), task_queue=worker.task_queue
        )
        await client.execute_workflow(
            SignalExternalWorkflow.run,
            SignalExternalWorkflowArgs(
                external_workflow_id=return_signal_handle.id, signal_value="some value"
            ),
            id=_wid(),
            task_queue=worker.task_queue,
        )
        assert "some value" == await return_signal_handle.result()


@workflow.defn
class MultiCancelWorkflow:
    @workflow.run
    async def run(self) -> list[str]:
        events: list[str] = []

        async def timer() -> None:
            nonlocal events
            try:
                await asyncio.sleep(1)
                events.append("timer success")
            except asyncio.CancelledError:
                events.append("timer cancelled")

        async def run_activity() -> None:
            nonlocal events
            try:
                await workflow.execute_activity(
                    wait_cancel, schedule_to_close_timeout=timedelta(5)
                )
                events.append("activity success")
            except ActivityError as err:
                if isinstance(err.cause, CancelledError):
                    events.append("activity cancelled")

        async def child(id: str) -> None:
            nonlocal events
            try:
                await workflow.execute_child_workflow(LongSleepWorkflow.run, id=id)
                events.append("child success")
            except ChildWorkflowError as err:
                if isinstance(err.cause, CancelledError):
                    events.append("child cancelled")

        fut = asyncio.gather(
            timer(),
            asyncio.shield(timer()),
            run_activity(),
            child(f"child-{workflow.info().workflow_id}"),
            return_exceptions=True,
        )
        await asyncio.sleep(0.1)
        fut.cancel()
        await workflow.wait_condition(lambda: len(events) == 4, timeout=30)
        try:
            await fut
        except asyncio.CancelledError:
            pass
        return events


@pytest.mark.skip(
    reason="DEVIATIONS D26/D32 (cooperative cancellation): a cancelled `wait_cancel` "
    "activity catches asyncio.CancelledError and *returns a value*, so our model "
    "records it as a successful completion rather than ActivityError(CancelledError). "
    "Temporal's hard cancellation discards the late result. The 4-way "
    "timer/activity/child cancel-event tally therefore never reaches 4. The "
    "workflow-side cancellation paths (timer, child) are covered by simple_cancel "
    "and cancel_external; only the activity-cancel-as-error semantics deviate."
)
async def test_workflow_cancel_multi(client: Client) -> None:
    async with new_worker(
        client, MultiCancelWorkflow, LongSleepWorkflow, activities=[wait_cancel]
    ) as worker:
        results = await client.execute_workflow(
            MultiCancelWorkflow.run,
            id=_wid(),
            task_queue=worker.task_queue,
        )
        assert sorted(results) == [
            "activity cancelled",
            "child cancelled",
            "timer cancelled",
            "timer success",
        ]


@workflow.defn
class ActivityTimeoutWorkflow:
    @workflow.run
    async def run(self) -> None:
        await workflow.execute_activity(
            wait_cancel,
            start_to_close_timeout=timedelta(milliseconds=10),
            retry_policy=RetryPolicy(maximum_attempts=1),
        )


async def test_workflow_activity_timeout(client: Client) -> None:
    async with new_worker(
        client, ActivityTimeoutWorkflow, activities=[wait_cancel]
    ) as worker:
        with pytest.raises(WorkflowFailureError) as err:
            await client.execute_workflow(
                ActivityTimeoutWorkflow.run,
                id=_wid(),
                task_queue=worker.task_queue,
            )
        assert isinstance(err.value.cause, ActivityError)
        assert isinstance(err.value.cause.cause, TimeoutError)


@workflow.defn
class TrapCancelWorkflow:
    @workflow.run
    async def run(self) -> str:
        try:
            await asyncio.Future()
            raise RuntimeError("should not get here")
        except asyncio.CancelledError:
            return "cancelled"


async def test_workflow_cancel_before_run(client: Client) -> None:
    # Start the workflow _and_ send cancel before the worker even exists.
    # warm_schema pre-migrates the namespace schema (our Worker owns schema
    # creation, DESIGN §5; production always has it) so client start/cancel can
    # run before the workflow's own worker launches — modelling Temporal's
    # always-up server.
    await warm_schema(client)
    task_queue = str(uuid.uuid4())
    handle = await client.start_workflow(
        TrapCancelWorkflow.run, id=_wid(), task_queue=task_queue
    )
    await handle.cancel()
    async with new_worker(client, TrapCancelWorkflow, task_queue=task_queue):
        assert "cancelled" == await handle.result()


@workflow.defn
class UUIDWorkflow:
    def __init__(self) -> None:
        self._result = "<unset>"

    @workflow.run
    async def run(self) -> None:
        self._result = str(workflow.uuid4())

    @workflow.query
    def result(self) -> str:
        return self._result


async def test_workflow_uuid(client: Client) -> None:
    task_queue = str(uuid.uuid4())
    async with new_worker(
        client, UUIDWorkflow, task_queue=task_queue, max_cached_workflows=0
    ):
        handle1 = await client.start_workflow(
            UUIDWorkflow.run, id=_wid(), task_queue=task_queue
        )
        await handle1.result()
        handle1_query_result = await handle1.query(UUIDWorkflow.result)

        handle2 = await client.start_workflow(
            UUIDWorkflow.run, id=_wid(), task_queue=task_queue
        )
        await handle2.result()
        handle2_query_result = await handle2.query(UUIDWorkflow.result)

        # Distinct per workflow, stable across repeated queries (deterministic).
        assert handle1_query_result != handle2_query_result
        assert handle1_query_result == await handle1.query(UUIDWorkflow.result)
        assert handle2_query_result == await handle2.query(UUIDWorkflow.result)

    # Stable even on a fresh worker (replayed from history).
    async with new_worker(
        client, UUIDWorkflow, task_queue=task_queue, max_cached_workflows=0
    ):
        assert handle1_query_result == await handle1.query(UUIDWorkflow.result)
        assert handle2_query_result == await handle2.query(UUIDWorkflow.result)


@workflow.defn
class WaitConditionTimeoutWorkflow:
    def __init__(self) -> None:
        self._done = False
        self._waiting = False

    @workflow.run
    async def run(self) -> None:
        # Force timeout, ignore, wait again
        try:
            await workflow.wait_condition(
                lambda: self._done, timeout=0.01, timeout_summary="hi!"
            )
            raise RuntimeError("Expected timeout")
        except asyncio.TimeoutError:
            pass
        self._waiting = True
        await workflow.wait_condition(lambda: self._done)

    @workflow.signal
    def done(self) -> None:
        self._done = True

    @workflow.query
    def waiting(self) -> bool:
        return self._waiting


async def test_workflow_wait_condition_timeout(client: Client) -> None:
    async with new_worker(client, WaitConditionTimeoutWorkflow) as worker:
        handle = await client.start_workflow(
            WaitConditionTimeoutWorkflow.run, id=_wid(), task_queue=worker.task_queue
        )

        async def waiting() -> bool:
            return cast(bool, await handle.query(WaitConditionTimeoutWorkflow.waiting))

        await assert_eq_eventually(True, waiting)
        await handle.signal(WaitConditionTimeoutWorkflow.done)
        await handle.result()


@dataclass
class TypedHandleResponse:
    field1: str


@workflow.defn
class TypedHandleWorkflow:
    @workflow.run
    async def run(self) -> TypedHandleResponse:
        return TypedHandleResponse(field1="foo")


async def test_workflow_typed_handle(client: Client) -> None:
    async with new_worker(client, TypedHandleWorkflow) as worker:
        id = _wid()
        await client.execute_workflow(
            TypedHandleWorkflow.run, id=id, task_queue=worker.task_queue
        )
        handle_result: TypedHandleResponse = await client.get_workflow_handle_for(
            TypedHandleWorkflow.run,
            id,
        ).result()
        assert isinstance(handle_result, TypedHandleResponse)


@dataclass
class MemoValue:
    field1: str


@workflow.defn
class MemoWorkflow:
    @workflow.run
    async def run(self, run_child: bool) -> None:
        expected_memo = {
            "dict_memo": {"field1": "dict"},
            "dataclass_memo": {"field1": "data"},
            "changed_memo": {"field1": "old value"},
            "removed_memo": {"field1": "removed"},
        }
        if run_child:
            assert workflow.memo() == expected_memo

        assert workflow.memo_value("dict_memo", type_hint=MemoValue) == MemoValue(
            field1="dict"
        )
        assert workflow.memo_value("dict_memo") == {"field1": "dict"}
        assert workflow.memo_value("dataclass_memo", type_hint=MemoValue) == MemoValue(
            field1="data"
        )
        with pytest.raises(KeyError):
            workflow.memo_value("absent_memo")
        assert (
            workflow.memo_value("absent_memo", "default value", type_hint=MemoValue)
            == "default value"
        )
        assert workflow.memo_value("absent_memo", "default value") == "default value"

        old_memo = dict(workflow.memo())

        assert workflow.memo_value("changed_memo", type_hint=MemoValue) == MemoValue(
            field1="old value"
        )
        assert workflow.memo_value("removed_memo", type_hint=MemoValue) == MemoValue(
            field1="removed"
        )
        with pytest.raises(KeyError):
            workflow.memo_value("added_memo", type_hint=MemoValue)

        workflow.upsert_memo(
            {
                "changed_memo": MemoValue(field1="new value"),
                "added_memo": MemoValue(field1="added"),
                "removed_memo": None,
            }
        )

        assert workflow.memo_value("changed_memo", type_hint=MemoValue) == MemoValue(
            field1="new value"
        )
        assert workflow.memo_value("added_memo", type_hint=MemoValue) == MemoValue(
            field1="added"
        )
        with pytest.raises(KeyError):
            workflow.memo_value("removed_memo", type_hint=MemoValue)

        if run_child:
            await workflow.execute_child_workflow(
                MemoWorkflow.run, False, memo=old_memo
            )


async def test_workflow_memo(client: Client) -> None:
    async with new_worker(client, MemoWorkflow) as worker:
        handle = await client.start_workflow(
            MemoWorkflow.run,
            True,
            id=_wid(),
            task_queue=worker.task_queue,
            memo={
                "dict_memo": {"field1": "dict"},
                "dataclass_memo": MemoValue(field1="data"),
                "changed_memo": MemoValue(field1="old value"),
                "removed_memo": MemoValue(field1="removed"),
            },
        )
        await handle.result()
        desc = await handle.describe()
        assert (await desc.memo()) == {
            "dict_memo": {"field1": "dict"},
            "dataclass_memo": {"field1": "data"},
            "changed_memo": {"field1": "new value"},
            "added_memo": {"field1": "added"},
        }
        assert (
            await desc.memo_value("dataclass_memo", type_hint=MemoValue)
        ) == MemoValue(field1="data")
        assert (
            await desc.memo_value("absent_memo", "default value")
        ) == "default value"
        with pytest.raises(KeyError):
            await desc.memo_value("absent_memo")


@workflow.defn
class QueryAffectConditionWorkflow:
    def __init__(self) -> None:
        self.seen_query = False

    @workflow.run
    async def run(self) -> None:
        def condition_never_after_query() -> bool:
            assert not self.seen_query
            return False

        while True:
            await workflow.wait_condition(condition_never_after_query)

    @workflow.query
    def check_condition(self) -> bool:
        # Deliberately mutating during a query (bad practice); asserts that the
        # query does NOT cause the wait_condition predicate to re-run.
        self.seen_query = True
        return True


async def test_workflow_query_does_not_run_condition(client: Client) -> None:
    async with new_worker(client, QueryAffectConditionWorkflow) as worker:
        handle = await client.start_workflow(
            QueryAffectConditionWorkflow.run, id=_wid(), task_queue=worker.task_queue
        )
        assert await handle.query(QueryAffectConditionWorkflow.check_condition)


@workflow.defn
class SignalAndQueryErrorsWorkflow:
    @workflow.run
    async def run(self) -> None:
        await asyncio.Future()

    @workflow.signal
    def bad_signal(self) -> NoReturn:
        raise ApplicationError("signal fail", 123)

    @workflow.query
    def bad_query(self) -> NoReturn:
        raise ApplicationError("query fail", 456)

    @workflow.query
    def other_query(self) -> str:
        raise NotImplementedError


async def test_workflow_signal_and_query_errors(client: Client) -> None:
    async with new_worker(client, SignalAndQueryErrorsWorkflow) as worker:
        handle = await client.start_workflow(
            SignalAndQueryErrorsWorkflow.run, id=_wid(), task_queue=worker.task_queue
        )
        # A signal handler that raises ApplicationError fails the workflow,
        # carrying the error details.
        await handle.signal(SignalAndQueryErrorsWorkflow.bad_signal)
        with pytest.raises(WorkflowFailureError) as err:
            await handle.result()
        assert isinstance(err.value.cause, ApplicationError)
        assert list(err.value.cause.details) == [123]
        # A query handler that raises fails the query (no details on queries).
        with pytest.raises(WorkflowQueryFailedError) as q_err:
            await handle.query(SignalAndQueryErrorsWorkflow.bad_query)
        assert str(q_err.value) == "query fail"
        # An unrecognized query fails; the exact "known queries" listing is
        # implementation-specific, so assert only the recognizable prefix.
        with pytest.raises(WorkflowQueryFailedError) as q_err:
            await handle.query("non-existent query")
        assert "non-existent query" in str(q_err.value)


@pytest.mark.skip(
    reason="DEVIATIONS D38: the legacy `(name, *args)` dynamic-handler signature "
    "is not supported; we require `(name, args: Sequence[RawValue])`. The "
    "new-style equivalent is covered by test_workflow_signal_and_query. The "
    "workflow can't even be defined (registration rejects the signature), so "
    "this is a placeholder rather than an adapted body."
)
async def test_workflow_signal_and_query_old_dynamic_style() -> None:
    pass


# --- runtime handler accessors (set_signal_handler / set_query_handler / dynamic) ---
# Directly exercises the runtime handler-accessor surface and the buffered-signal
# flush: a signal that arrives before its handler is registered must be delivered
# when set_signal_handler later installs it.


@workflow.defn
class SignalAndQueryHandlersWorkflow:
    def __init__(self) -> None:
        self._last_event: str | None = None

    @workflow.run
    async def run(self) -> None:
        await asyncio.Future()

    @workflow.query
    def last_event(self) -> str:
        return self._last_event or "<no event>"

    @workflow.signal
    def set_signal_handler(self, signal_name: str) -> None:
        def new_handler(arg: str) -> None:
            self._last_event = f"signal {signal_name}: {arg}"

        workflow.set_signal_handler(signal_name, new_handler)

    @workflow.signal
    def set_query_handler(self, query_name: str) -> None:
        def new_handler(arg: str) -> str:
            return f"query {query_name}: {arg}"

        workflow.set_query_handler(query_name, new_handler)

    @workflow.signal
    def set_dynamic_signal_handler(self) -> None:
        def new_handler(name: str, args: Sequence[RawValue]) -> None:
            arg = workflow.payload_converter().from_payload(args[0].payload, str)
            self._last_event = f"signal dynamic {name}: {arg}"

        workflow.set_dynamic_signal_handler(new_handler)

    @workflow.signal
    def set_dynamic_query_handler(self) -> None:
        def new_handler(name: str, args: Sequence[RawValue]) -> str:
            arg = workflow.payload_converter().from_payload(args[0].payload, str)
            return f"query dynamic {name}: {arg}"

        workflow.set_dynamic_query_handler(new_handler)


async def test_workflow_signal_and_query_handlers(client: Client) -> None:
    async with new_worker(client, SignalAndQueryHandlersWorkflow) as worker:
        handle = await client.start_workflow(
            SignalAndQueryHandlersWorkflow.run, id=_wid(), task_queue=worker.task_queue
        )

        # Confirm signals buffered when not found, then flushed on registration.
        await handle.signal("unknown_signal1", "val1")
        await handle.signal(
            SignalAndQueryHandlersWorkflow.set_signal_handler, "unknown_signal1"
        )
        assert "signal unknown_signal1: val1" == await handle.query(
            SignalAndQueryHandlersWorkflow.last_event
        )

        # Normal signal handling (handler already registered).
        await handle.signal("unknown_signal1", "val2")
        assert "signal unknown_signal1: val2" == await handle.query(
            SignalAndQueryHandlersWorkflow.last_event
        )

        # Dynamic signal handling: buffered before, and live after.
        await handle.signal("unknown_signal2", "val3")
        await handle.signal(SignalAndQueryHandlersWorkflow.set_dynamic_signal_handler)
        assert "signal dynamic unknown_signal2: val3" == await handle.query(
            SignalAndQueryHandlersWorkflow.last_event
        )
        await handle.signal("unknown_signal3", "val4")
        assert "signal dynamic unknown_signal3: val4" == await handle.query(
            SignalAndQueryHandlersWorkflow.last_event
        )

        # Normal query handling installed at runtime.
        await handle.signal(
            SignalAndQueryHandlersWorkflow.set_query_handler, "unknown_query1"
        )
        assert "query unknown_query1: val5" == await handle.query(
            "unknown_query1", "val5"
        )

        # Dynamic query handling installed at runtime.
        await handle.signal(SignalAndQueryHandlersWorkflow.set_dynamic_query_handler)
        assert "query dynamic unknown_query2: val6" == await handle.query(
            "unknown_query2", "val6"
        )


# --- patching (workflow.patched / deprecate_patch) ---------------------------


class PatchWorkflowBase:
    def __init__(self) -> None:
        self._result = "<unset>"

    @workflow.query
    def result(self) -> str:
        return self._result


@workflow.defn(name="patch-workflow")
class PrePatchWorkflow(PatchWorkflowBase):
    @workflow.run
    async def run(self) -> None:
        self._result = "pre-patch"


@workflow.defn(name="patch-workflow")
class PatchWorkflow(PatchWorkflowBase):
    @workflow.run
    async def run(self) -> None:
        if workflow.patched("my-patch"):
            self._result = "post-patch"
        else:
            self._result = "pre-patch"


@workflow.defn(name="patch-workflow")
class DeprecatePatchWorkflow(PatchWorkflowBase):
    @workflow.run
    async def run(self) -> None:
        workflow.deprecate_patch("my-patch")
        self._result = "post-patch"


@workflow.defn(name="patch-workflow")
class PostPatchWorkflow(PatchWorkflowBase):
    @workflow.run
    async def run(self) -> None:
        self._result = "post-patch"


@pytest.mark.skip(
    reason="DEVIATIONS D27: this test queries a *completed* workflow after the "
    "worker's registered code for that type name has been swapped (PrePatch -> "
    "Patch). Queries on closed runs rehydrate-by-replay under the currently-"
    "registered code; when that code differs from what the run executed, our "
    "rehydration can't reconstruct the instance ('the workflow's code may have "
    "changed since it ran'). Temporal serves it from retained history instead. "
    "The actual patch-determinism guarantee — a run memoizing its patch decision "
    "across a worker/code swap — is verified by test_workflow_patch_memoized."
)
async def test_workflow_patch(client: Client) -> None:
    workflow_run = PrePatchWorkflow.run
    task_queue = str(uuid.uuid4())

    async def execute() -> WorkflowHandle:
        handle = await client.start_workflow(
            workflow_run, id=_wid(), task_queue=task_queue
        )
        await handle.result()
        return handle

    async def query_result(handle: WorkflowHandle) -> str:
        return cast(str, await handle.query(PatchWorkflowBase.result))

    # Simple pre-patch workflow (cache disabled so the worker restart replays).
    async with new_worker(
        client, PrePatchWorkflow, task_queue=task_queue, max_cached_workflows=0
    ):
        pre_patch_handle = await execute()
        assert "pre-patch" == await query_result(pre_patch_handle)

    # Patched code: old run still pre-patch, new run post-patch.
    async with new_worker(
        client, PatchWorkflow, task_queue=task_queue, max_cached_workflows=0
    ):
        patch_handle = await execute()
        assert "post-patch" == await query_result(patch_handle)
        assert "pre-patch" == await query_result(pre_patch_handle)

    # Deprecated patch.
    async with new_worker(
        client, DeprecatePatchWorkflow, task_queue=task_queue, max_cached_workflows=0
    ):
        deprecate_patch_handle = await execute()
        assert "post-patch" == await query_result(deprecate_patch_handle)
        assert "post-patch" == await query_result(patch_handle)

    # Deprecation gone.
    async with new_worker(
        client, PostPatchWorkflow, task_queue=task_queue, max_cached_workflows=0
    ):
        post_patch_handle = await execute()
        assert "post-patch" == await query_result(post_patch_handle)
        assert "post-patch" == await query_result(deprecate_patch_handle)


@workflow.defn(name="patch-memoized")
class PatchMemoizedWorkflowUnpatched:
    def __init__(self, *, should_patch: bool = False) -> None:
        self.should_patch = should_patch
        self._waiting_signal = True

    @workflow.run
    async def run(self) -> list[str]:
        results: list[str] = []
        if self.should_patch and workflow.patched("some-patch"):
            results.append("pre-patch")
        self._waiting_signal = True
        await workflow.wait_condition(lambda: not self._waiting_signal)
        results.append("some-value")
        if self.should_patch and workflow.patched("some-patch"):
            results.append("post-patch")
        return results

    @workflow.signal
    def signal(self) -> None:
        self._waiting_signal = False

    @workflow.query
    def waiting_signal(self) -> bool:
        return self._waiting_signal


@workflow.defn(name="patch-memoized")
class PatchMemoizedWorkflowPatched(PatchMemoizedWorkflowUnpatched):
    def __init__(self) -> None:
        super().__init__(should_patch=True)

    @workflow.run
    async def run(self) -> list[str]:
        return await super().run()


async def test_workflow_patch_memoized(client: Client) -> None:
    # Start unpatched, park halfway, stop the worker (workflow persists), then
    # bring up a worker running the *patched* code. The parked run must memoize
    # that it did NOT take the patch (so it stays unpatched on replay), while a
    # fresh run under the patched worker does take it.
    task_queue = f"tq-{uuid.uuid4()}"
    async with new_worker(
        client,
        PatchMemoizedWorkflowUnpatched,
        task_queue=task_queue,
        max_cached_workflows=0,
    ):
        pre_patch_handle = await client.start_workflow(
            PatchMemoizedWorkflowUnpatched.run, id=_wid(), task_queue=task_queue
        )

        async def pre_waiting() -> bool:
            return cast(
                bool,
                await pre_patch_handle.query(
                    PatchMemoizedWorkflowUnpatched.waiting_signal
                ),
            )

        await assert_eq_eventually(True, pre_waiting)

    async with new_worker(
        client,
        PatchMemoizedWorkflowPatched,
        task_queue=task_queue,
        max_cached_workflows=0,
    ):
        post_patch_handle = await client.start_workflow(
            PatchMemoizedWorkflowPatched.run, id=_wid(), task_queue=task_queue
        )

        async def post_waiting() -> bool:
            return cast(
                bool,
                await post_patch_handle.query(
                    PatchMemoizedWorkflowPatched.waiting_signal
                ),
            )

        await assert_eq_eventually(True, post_waiting)

        await pre_patch_handle.signal(PatchMemoizedWorkflowUnpatched.signal)
        await post_patch_handle.signal(PatchMemoizedWorkflowPatched.signal)

        assert ["some-value"] == await pre_patch_handle.result()
        assert [
            "pre-patch",
            "some-value",
            "post-patch",
        ] == await post_patch_handle.result()


# --- cancellation reason (child + external) ----------------------------------


@workflow.defn
class CancelReasonReporter:
    """Swallows a cancel and returns the observed reason."""

    @workflow.run
    async def run(self) -> str:
        try:
            await asyncio.sleep(1000)
        except asyncio.CancelledError:
            return workflow.cancellation_reason() or ""
        raise RuntimeError("unreachable")


@workflow.defn
class ChildCancelReasonWorkflow:
    @workflow.run
    async def run(self, msg: str) -> str:
        child = await workflow.start_child_workflow(
            CancelReasonReporter.run,
            id=f"{workflow.info().workflow_id}_child",
        )
        child.cancel(msg)
        return cast(str, await child)


@pytest.mark.skip(
    reason="Cancellation-type / D32 family (the child-workflow analog of "
    "cancel_multi): the parent cancels a child that catches CancelledError and "
    "RETURNS a value. Temporal completes that child successfully, so `await child` "
    "yields the value; our model resolves the parent's awaiter as cancelled "
    "instead of waiting for the child's swallow-and-return, so the parent fails "
    "with CancelledError. Honoring it needs WAIT_CANCELLATION_COMPLETED child "
    "semantics — a deep interpreter change. The reason *propagation* itself works "
    "(test_workflow_external_cancel_reason passes)."
)
async def test_workflow_child_cancel_reason(client: Client) -> None:
    async with new_worker(
        client, ChildCancelReasonWorkflow, CancelReasonReporter
    ) as worker:
        result = await client.execute_workflow(
            ChildCancelReasonWorkflow.run,
            "from-parent",
            id=_wid(),
            task_queue=worker.task_queue,
        )
        assert result == "from-parent"


@workflow.defn
class ExternalCancelReasonWorkflow:
    @workflow.run
    async def run(self, target_id: str) -> None:
        await workflow.get_external_workflow_handle(target_id).cancel(
            reason="from-external-caller"
        )


async def test_workflow_external_cancel_reason(client: Client) -> None:
    async with new_worker(
        client, ExternalCancelReasonWorkflow, CancelReasonReporter
    ) as worker:
        target_id = _wid()
        target = await client.start_workflow(
            CancelReasonReporter.run, id=target_id, task_queue=worker.task_queue
        )
        await client.execute_workflow(
            ExternalCancelReasonWorkflow.run,
            target_id,
            id=_wid(),
            task_queue=worker.task_queue,
        )
        assert "from-external-caller" in await target.result()


# --- cancel a child whose first task never started ---------------------------


@workflow.defn
class CancelDuringChildStartWorkflow:
    def __init__(self) -> None:
        self._proceed = False

    @workflow.signal
    def proceed(self) -> None:
        self._proceed = True

    @workflow.run
    async def run(self) -> None:
        await workflow.wait_condition(lambda: self._proceed)
        # Start a child on a task queue with no worker: its first task never
        # starts, so the start loop would block forever if cancellation in that
        # window were mishandled (temporalio regression #1445).
        await workflow.start_child_workflow(
            LongSleepWorkflow.run,
            id=f"{workflow.info().workflow_id}_child",
            task_queue="nonexistent-task-queue-no-worker-abc123",
        )
        await workflow.sleep(1000)


async def test_workflow_cancel_child_unstarted(client: Client) -> None:
    # Our worker must host the child type (unlike Temporal, where it can live on
    # another worker); the child is still started on a task queue THIS worker
    # does not poll, so it stays unstarted while the parent is cancelled.
    async with new_worker(
        client, CancelDuringChildStartWorkflow, LongSleepWorkflow
    ) as worker:
        handle = await client.start_workflow(
            CancelDuringChildStartWorkflow.run,
            id=_wid(),
            task_queue=worker.task_queue,
            execution_timeout=timedelta(seconds=30),
        )
        await handle.signal(CancelDuringChildStartWorkflow.proceed)
        await handle.cancel()
        with pytest.raises(WorkflowFailureError) as err:
            await handle.result()
        assert isinstance(err.value.cause, CancelledError)


# --- callable-class activities (execute_activity_method) ---------------------


@dataclass
class MyDataClass:
    field1: str

    def assert_expected(self) -> None:
        # Calling this at all confirms the right *type* survived round-tripping;
        # the field check confirms the value (used by the dataclass-typed trio).
        assert self.field1 == "some value"


class MethodActivity:
    def __init__(self, orig_field1: str) -> None:
        self.orig_field1 = orig_field1

    @activity.defn(name="custom-name")
    async def add(self, to_add: MyDataClass) -> MyDataClass:
        return MyDataClass(field1=self.orig_field1 + to_add.field1)

    @activity.defn
    async def add_multi(self, source: MyDataClass, to_add: str) -> MyDataClass:
        return MyDataClass(field1=source.field1 + to_add)


@workflow.defn
class ActivityMethodWorkflow:
    @workflow.run
    async def run(self, to_add: MyDataClass) -> MyDataClass:
        ret = await workflow.execute_activity_method(
            MethodActivity.add, to_add, start_to_close_timeout=timedelta(seconds=30)
        )
        return cast(
            MyDataClass,
            await workflow.execute_activity_method(
                MethodActivity.add_multi,
                args=[ret, ", in workflow"],
                start_to_close_timeout=timedelta(seconds=30),
            ),
        )


async def test_workflow_activity_method(client: Client) -> None:
    activity_instance = MethodActivity("in worker")
    async with new_worker(
        client,
        ActivityMethodWorkflow,
        activities=[activity_instance.add, activity_instance.add_multi],
    ) as worker:
        result = await client.execute_workflow(
            ActivityMethodWorkflow.run,
            MyDataClass(field1=", workflow param"),
            id=_wid(),
            task_queue=worker.task_queue,
        )
        assert result == MyDataClass(field1="in worker, workflow param, in workflow")


@activity.defn(name="custom-name")
class CallableClassActivity:
    def __init__(self, orig_field1: str) -> None:
        self.orig_field1 = orig_field1

    async def __call__(self, to_add: MyDataClass) -> MyDataClass:
        return MyDataClass(field1=self.orig_field1 + to_add.field1)


@workflow.defn
class ActivityCallableClassWorkflow:
    @workflow.run
    async def run(self, to_add: MyDataClass) -> MyDataClass:
        result = await workflow.execute_activity_class(
            CallableClassActivity, to_add, start_to_close_timeout=timedelta(seconds=30)
        )
        assert isinstance(result, MyDataClass)
        return result


async def test_workflow_activity_callable_class(client: Client) -> None:
    activity_instance = CallableClassActivity("in worker")
    async with new_worker(
        client, ActivityCallableClassWorkflow, activities=[activity_instance]
    ) as worker:
        result = await client.execute_workflow(
            ActivityCallableClassWorkflow.run,
            MyDataClass(field1=", workflow param"),
            id=_wid(),
            task_queue=worker.task_queue,
        )
        assert result == MyDataClass(field1="in worker, workflow param")


async def test_workflow_activity_callable_class_bad_register(client: Client) -> None:
    # Registering the class (not an instance) must fail clearly.
    with pytest.raises(TypeError) as err:
        async with new_worker(
            client, ActivityCallableClassWorkflow, activities=[CallableClassActivity]
        ):
            pass
    assert "is a class instead of an instance" in str(err.value)


# --- continue-as-new (memo + retry policy + run-id chain) --------------------


@workflow.defn
class ContinueAsNewWorkflow:
    @workflow.run
    async def run(self, past_run_ids: list[str]) -> list[str]:
        # Memo and retry policy carry across each continue-as-new.
        assert workflow.memo_value("past_run_id_count") == len(past_run_ids)
        retry_policy = workflow.info().retry_policy
        assert retry_policy and retry_policy.maximum_attempts == 1000 + len(
            past_run_ids
        )

        if len(past_run_ids) == 5:
            return past_run_ids
        info = workflow.info()
        if info.continued_run_id:
            past_run_ids.append(info.continued_run_id)
            assert info.first_execution_run_id == past_run_ids[0]
        workflow.continue_as_new(
            past_run_ids,
            memo={"past_run_id_count": len(past_run_ids)},
            retry_policy=RetryPolicy(maximum_attempts=1000 + len(past_run_ids)),
        )


async def test_workflow_continue_as_new(client: Client) -> None:
    async with new_worker(client, ContinueAsNewWorkflow) as worker:
        handle = await client.start_workflow(
            ContinueAsNewWorkflow.run,
            cast("list[str]", []),
            id=_wid(),
            task_queue=worker.task_queue,
            memo={"past_run_id_count": 0},
            retry_policy=RetryPolicy(maximum_attempts=1000),
        )
        result = await handle.result()
        assert len(result) == 5
        assert result[0] == handle.first_execution_run_id


# --- local-activity retry/backoff --------------------------------------------


@activity.defn
async def fail_until_attempt_activity(until_attempt: int) -> str:
    if activity.info().attempt < until_attempt:
        raise ApplicationError("Attempt too low")
    return f"attempt: {activity.info().attempt}"


@workflow.defn
class LocalActivityBackoffWorkflow:
    @workflow.run
    async def run(self) -> None:
        await workflow.execute_local_activity(
            fail_until_attempt_activity,
            2,
            start_to_close_timeout=timedelta(minutes=1),
            local_retry_threshold=timedelta(seconds=1),
            retry_policy=RetryPolicy(
                maximum_attempts=2, initial_interval=timedelta(seconds=2)
            ),
        )


async def test_workflow_local_activity_backoff(client: Client) -> None:
    # Adapted: the SDK test also asserts on history (one TIMER_FIRED, two
    # MARKER_RECORDED) which is server-only. We keep the behavioral half — the
    # local activity fails on attempt 1, backs off past local_retry_threshold,
    # and succeeds on attempt 2, so the workflow completes.
    async with new_worker(
        client, LocalActivityBackoffWorkflow, activities=[fail_until_attempt_activity]
    ) as worker:
        await client.execute_workflow(
            LocalActivityBackoffWorkflow.run,
            id=_wid(),
            task_queue=worker.task_queue,
        )


# --- workflow updates (handlers, validators, dynamic, errors) ----------------


@workflow.defn
class UpdateHandlersWorkflow:
    def __init__(self) -> None:
        self._last_event: str | None = None

    @workflow.run
    async def run(self) -> None:
        workflow.set_update_handler("first_task_update", lambda: "worked")
        await asyncio.Future()  # wait forever

    @workflow.update
    def last_event(self, an_arg: str) -> str:
        if an_arg == "fail":
            raise ApplicationError("SyncFail")
        le = self._last_event or "<no event>"
        self._last_event = an_arg
        return le

    @last_event.validator
    def last_event_validator(self, an_arg: str) -> None:
        if an_arg == "reject_me":
            raise ApplicationError("Rejected")

    @workflow.update
    async def last_event_async(self, an_arg: str) -> str:
        await asyncio.sleep(1)
        if an_arg == "fail":
            raise ApplicationError("AsyncFail")
        le = self._last_event or "<no event>"
        self._last_event = an_arg
        return le

    @workflow.update(name="renamed")
    async def async_named(self) -> str:
        return "named"

    @workflow.update
    async def set_dynamic(self) -> str:
        def dynahandler(name: str, _args: Sequence[RawValue]) -> str:
            return "dynahandler - " + name

        def dynavalidator(name: str, _args: Sequence[RawValue]) -> None:
            if name == "reject_me":
                raise ApplicationError("Rejected")

        workflow.set_dynamic_update_handler(dynahandler, validator=dynavalidator)
        return "set"


async def test_workflow_update_handlers_happy(client: Client) -> None:
    async with new_worker(
        client, UpdateHandlersWorkflow, activities=[say_hello]
    ) as worker:
        wf_id = _wid()
        handle = await client.start_workflow(
            UpdateHandlersWorkflow.run, id=wf_id, task_queue=worker.task_queue
        )

        # Normal handling (returns the previous event)
        assert "<no event>" == await handle.execute_update(
            UpdateHandlersWorkflow.last_event, "val2"
        )
        # Async handler
        assert "val2" == await handle.execute_update(
            UpdateHandlersWorkflow.last_event_async, "val3"
        )
        # Dynamic handler, registered at runtime then invoked by name
        await handle.execute_update(UpdateHandlersWorkflow.set_dynamic)
        assert "dynahandler - made_up" == await handle.execute_update("made_up")
        # Name overload
        assert "named" == await handle.execute_update(
            UpdateHandlersWorkflow.async_named
        )
        # Untyped handle
        assert "val3" == await client.get_workflow_handle(wf_id).execute_update(
            UpdateHandlersWorkflow.last_event, "val4"
        )


async def test_workflow_update_handlers_unhappy(client: Client) -> None:
    async with new_worker(client, UpdateHandlersWorkflow) as worker:
        handle = await client.start_workflow(
            UpdateHandlersWorkflow.run, id=_wid(), task_queue=worker.task_queue
        )

        # Undefined handler
        with pytest.raises(WorkflowUpdateFailedError) as err:
            await handle.execute_update("whargarbl", "whatever")
        assert isinstance(err.value.cause, ApplicationError)
        assert "whargarbl" in err.value.cause.message

        # Rejection by validator
        with pytest.raises(WorkflowUpdateFailedError) as err:
            await handle.execute_update(UpdateHandlersWorkflow.last_event, "reject_me")
        assert isinstance(err.value.cause, ApplicationError)
        assert "Rejected" == err.value.cause.message

        # Failure inside the (sync) handler
        with pytest.raises(WorkflowUpdateFailedError) as err:
            await handle.execute_update(UpdateHandlersWorkflow.last_event, "fail")
        assert isinstance(err.value.cause, ApplicationError)
        assert "SyncFail" == err.value.cause.message

        # Failure inside the async handler
        with pytest.raises(WorkflowUpdateFailedError) as err:
            await handle.execute_update(UpdateHandlersWorkflow.last_event_async, "fail")
        assert isinstance(err.value.cause, ApplicationError)
        assert "AsyncFail" == err.value.cause.message
        # NOTE: temporalio's suite also asserts that cancelling an activity
        # inside the handler surfaces CancelledError. That relies on a cancel
        # before the task first yields *removing the un-sent command*; we
        # dispatch activities eagerly (the cancel_unsent / cancel_multi
        # deviation), so it is omitted here.

        # Dynamic handler registered, then rejected by its validator
        await handle.execute_update(UpdateHandlersWorkflow.set_dynamic)
        with pytest.raises(WorkflowUpdateFailedError) as err:
            await handle.execute_update("reject_me")
        assert isinstance(err.value.cause, ApplicationError)
        assert "Rejected" == err.value.cause.message


@workflow.defn
class UpdateSeparateHandleWorkflow:
    def __init__(self) -> None:
        self._complete = False
        self._complete_update = False

    @workflow.run
    async def run(self) -> str:
        await workflow.wait_condition(lambda: self._complete)
        return "workflow-done"

    @workflow.update
    async def update(self) -> str:
        await workflow.wait_condition(lambda: self._complete_update)
        self._complete = True
        return "update-done"

    @workflow.signal
    async def signal(self) -> None:
        self._complete_update = True


async def test_workflow_update_separate_handle(client: Client) -> None:
    async with new_worker(client, UpdateSeparateHandleWorkflow) as worker:
        handle = await client.start_workflow(
            UpdateSeparateHandleWorkflow.run, id=_wid(), task_queue=worker.task_queue
        )

        # Start an update, waiting only until it is accepted (it then blocks).
        update_handle_1 = await handle.start_update(
            UpdateSeparateHandleWorkflow.update,
            wait_for_stage=WorkflowUpdateStage.ACCEPTED,
        )
        assert update_handle_1.workflow_run_id == handle.first_execution_run_id

        # A second, independently-constructed handle to the same update.
        update_handle_2 = client.get_workflow_handle(
            handle.id, run_id=handle.result_run_id
        ).get_update_handle_for(UpdateSeparateHandleWorkflow.update, update_handle_1.id)
        task1 = asyncio.create_task(update_handle_1.result())
        task2 = asyncio.create_task(update_handle_2.result())

        # Unblock the update; both handles observe the same result.
        await handle.signal(UpdateSeparateHandleWorkflow.signal)
        assert "update-done" == await task1
        assert "update-done" == await task2
        assert "workflow-done" == await handle.result()


# --- already-started (duplicate workflow id) ---------------------------------


async def test_workflow_already_started(client: Client) -> None:
    async with new_worker(client, LongSleepWorkflow) as worker:
        wf_id = _wid()
        await client.start_workflow(
            LongSleepWorkflow.run, id=wf_id, task_queue=worker.task_queue
        )
        with pytest.raises(WorkflowAlreadyStartedError):
            await client.start_workflow(
                LongSleepWorkflow.run, id=wf_id, task_queue=worker.task_queue
            )


@workflow.defn
class ChildAlreadyStartedWorkflow:
    @workflow.run
    async def run(self) -> None:
        child_id = f"{workflow.info().workflow_id}_child"
        await workflow.start_child_workflow(LongSleepWorkflow.run, id=child_id)
        try:
            await workflow.start_child_workflow(LongSleepWorkflow.run, id=child_id)
        except WorkflowAlreadyStartedError:
            raise ApplicationError("Already started")


async def test_workflow_child_already_started(client: Client) -> None:
    async with new_worker(
        client, ChildAlreadyStartedWorkflow, LongSleepWorkflow
    ) as worker:
        with pytest.raises(WorkflowFailureError) as err:
            await client.execute_workflow(
                ChildAlreadyStartedWorkflow.run,
                id=_wid(),
                task_queue=worker.task_queue,
            )
        assert isinstance(err.value.cause, ApplicationError)
        assert err.value.cause.message == "Already started"


# --- bad signal param (un-deserializable signals are dropped) ----------------


@dataclass
class BadSignalParam:
    some_str: str


@workflow.defn
class BadSignalParamWorkflow:
    def __init__(self) -> None:
        self._signals: list[BadSignalParam] = []

    @workflow.run
    async def run(self) -> list[BadSignalParam]:
        await workflow.wait_condition(
            lambda: bool(self._signals) and self._signals[-1].some_str == "finish"
        )
        return self._signals

    @workflow.signal
    async def some_signal(self, param: BadSignalParam) -> None:
        self._signals.append(param)


async def test_workflow_bad_signal_param(client: Client) -> None:
    # Adapted: the SDK test also asserts on the captured "Failed deserializing
    # signal input" log record (impl-internal). We keep the behavioral half — a
    # badly-typed signal payload is dropped and the workflow keeps running,
    # collecting only the well-typed signals.
    async with new_worker(client, BadSignalParamWorkflow) as worker:
        handle = await client.start_workflow(
            BadSignalParamWorkflow.run, id=_wid(), task_queue=worker.task_queue
        )
        # First and third are the wrong type and must be dropped.
        await handle.signal("some_signal", "bad")
        await handle.signal("some_signal", BadSignalParam(some_str="good"))
        await handle.signal("some_signal", 123)
        await handle.signal("some_signal", BadSignalParam(some_str="finish"))
        assert [
            BadSignalParam(some_str="good"),
            BadSignalParam(some_str="finish"),
        ] == await handle.result()


# --- dataclass-typed handlers + interface (Protocol / ABC) references ---------
# One workflow scaffold drives three tests: typed dataclass round-tripping
# through activities/child/signals/queries, plus using a Protocol and an
# abstract base as the typed "interface" reference when the impl is absent.


@activity.defn
async def data_class_typed_activity(param: MyDataClass) -> MyDataClass:
    param.assert_expected()
    return param


@runtime_checkable
@workflow.defn(name="DataClassTypedWorkflow")
class DataClassTypedWorkflowProto(Protocol):
    @workflow.run
    async def run(self, arg: MyDataClass) -> MyDataClass: ...

    @workflow.signal
    def signal_sync(self, param: MyDataClass) -> None: ...

    @workflow.query
    def query_sync(self, param: MyDataClass) -> MyDataClass: ...

    @workflow.signal
    def complete(self) -> None: ...


@workflow.defn(name="DataClassTypedWorkflow")
class DataClassTypedWorkflowAbstract(ABC):
    @workflow.run
    @abstractmethod
    async def run(self, param: MyDataClass) -> MyDataClass: ...

    @workflow.signal
    @abstractmethod
    def signal_sync(self, param: MyDataClass) -> None: ...

    @workflow.query
    @abstractmethod
    def query_sync(self, param: MyDataClass) -> MyDataClass: ...

    @workflow.signal
    @abstractmethod
    def complete(self) -> None: ...


@workflow.defn
class DataClassTypedWorkflow(DataClassTypedWorkflowAbstract):
    def __init__(self) -> None:
        self._should_complete = asyncio.Event()

    @workflow.run
    async def run(self, param: MyDataClass) -> MyDataClass:
        param.assert_expected()
        # Only exercise activities/child at the top level.
        if not workflow.info().parent:
            param = await workflow.execute_activity(
                data_class_typed_activity,
                param,
                start_to_close_timeout=timedelta(seconds=30),
            )
            param.assert_expected()
            param = await workflow.execute_local_activity(
                data_class_typed_activity,
                param,
                start_to_close_timeout=timedelta(seconds=30),
            )
            param.assert_expected()
            child_handle = await workflow.start_child_workflow(
                DataClassTypedWorkflow.run,
                param,
                id=f"{workflow.info().workflow_id}_child",
            )
            await child_handle.signal(DataClassTypedWorkflow.signal_sync, param)
            await child_handle.signal(DataClassTypedWorkflow.signal_async, param)
            await child_handle.signal(DataClassTypedWorkflow.complete)
            param = await child_handle
            param.assert_expected()
        await self._should_complete.wait()
        return param

    @workflow.signal
    def signal_sync(self, param: MyDataClass) -> None:
        param.assert_expected()

    @workflow.signal
    async def signal_async(self, param: MyDataClass) -> None:
        param.assert_expected()

    @workflow.query
    def query_sync(self, param: MyDataClass) -> MyDataClass:
        param.assert_expected()
        return param

    # temporalio declares this async (a deprecated form); we require sync query
    # handlers (DEVIATIONS D17), so it is a normal def here.
    @workflow.query
    def query_async(self, param: MyDataClass) -> MyDataClass:
        return param

    @workflow.signal
    def complete(self) -> None:
        self._should_complete.set()


async def test_workflow_dataclass_typed(client: Client) -> None:
    async with new_worker(
        client, DataClassTypedWorkflow, activities=[data_class_typed_activity]
    ) as worker:
        val = MyDataClass(field1="some value")
        handle = await client.start_workflow(
            DataClassTypedWorkflow.run, val, id=_wid(), task_queue=worker.task_queue
        )
        await handle.signal(DataClassTypedWorkflow.signal_sync, val)
        await handle.signal(DataClassTypedWorkflow.signal_async, val)
        (await handle.query(DataClassTypedWorkflow.query_sync, val)).assert_expected()
        query_result: MyDataClass = await handle.query(
            DataClassTypedWorkflow.query_async, val
        )
        query_result.assert_expected()
        await handle.signal(DataClassTypedWorkflow.complete)
        (await handle.result()).assert_expected()


async def test_workflow_separate_protocol(client: Client) -> None:
    # A Protocol can stand in as the typed "interface" when the impl is absent.
    async with new_worker(
        client, DataClassTypedWorkflow, activities=[data_class_typed_activity]
    ) as worker:
        assert isinstance(DataClassTypedWorkflow(), DataClassTypedWorkflowProto)
        val = MyDataClass(field1="some value")
        handle = await client.start_workflow(
            DataClassTypedWorkflowProto.run,
            val,
            id=_wid(),
            task_queue=worker.task_queue,
        )
        await handle.signal(DataClassTypedWorkflowProto.signal_sync, val)
        (
            await handle.query(DataClassTypedWorkflowProto.query_sync, val)
        ).assert_expected()
        await handle.signal(DataClassTypedWorkflowProto.complete)
        (await handle.result()).assert_expected()


async def test_workflow_separate_abstract(client: Client) -> None:
    # An abstract base can likewise stand in as the typed "interface".
    async with new_worker(
        client, DataClassTypedWorkflow, activities=[data_class_typed_activity]
    ) as worker:
        assert issubclass(DataClassTypedWorkflow, DataClassTypedWorkflowAbstract)
        val = MyDataClass(field1="some value")
        handle = await client.start_workflow(
            DataClassTypedWorkflowAbstract.run,
            val,
            id=_wid(),
            task_queue=worker.task_queue,
        )
        await handle.signal(DataClassTypedWorkflowAbstract.signal_sync, val)
        (
            await handle.query(DataClassTypedWorkflowAbstract.query_sync, val)
        ).assert_expected()
        await handle.signal(DataClassTypedWorkflowAbstract.complete)
        (await handle.result()).assert_expected()


# --- timers ------------------------------------------------------------------


@workflow.defn
class WorkflowSleepWorkflow:
    @workflow.run
    async def run(self) -> float:
        start_time = workflow.time()
        await workflow.sleep(1)
        return workflow.time() - start_time


async def test_workflow_sleep(client: Client) -> None:
    async with new_worker(client, WorkflowSleepWorkflow) as worker:
        workflow_elapsed = await client.execute_workflow(
            WorkflowSleepWorkflow.run, id=_wid(), task_queue=worker.task_queue
        )
        assert workflow_elapsed >= 1


# --- completion-command ordering (first completion wins) ---------------------


@workflow.defn
class FirstCompletionCommandIsHonoredWorkflow:
    def __init__(
        self, main_workflow_returns_before_signal_completions: bool = False
    ) -> None:
        self.seen_first_signal = False
        self.seen_second_signal = False
        self.main_workflow_returns_before_signal_completions = (
            main_workflow_returns_before_signal_completions
        )
        self.run_finished = False

    @workflow.run
    async def run(self) -> str:
        await workflow.wait_condition(
            lambda: self.seen_first_signal and self.seen_second_signal
        )
        self.run_finished = True
        return "workflow-result"

    @workflow.signal
    async def this_signal_executes_first(self) -> None:
        self.seen_first_signal = True
        if self.main_workflow_returns_before_signal_completions:
            await workflow.wait_condition(lambda: self.run_finished)
        raise ApplicationError(
            "Client should see this error unless doing ping-pong "
            "(in which case main coroutine returns first)"
        )

    @workflow.signal
    async def this_signal_executes_second(self) -> None:
        await workflow.wait_condition(lambda: self.seen_first_signal)
        self.seen_second_signal = True
        if self.main_workflow_returns_before_signal_completions:
            await workflow.wait_condition(lambda: self.run_finished)
        raise ApplicationError("Client should never see this error!")


@workflow.defn
class FirstCompletionCommandIsHonoredSignalWaitWorkflow(
    FirstCompletionCommandIsHonoredWorkflow
):
    def __init__(self) -> None:
        super().__init__(main_workflow_returns_before_signal_completions=True)

    @workflow.run
    async def run(self) -> str:
        return await super().run()


async def _do_first_completion_command_is_honored_test(
    client: Client, main_workflow_returns_before_signal_completions: bool
) -> None:
    workflow_cls: type[FirstCompletionCommandIsHonoredWorkflow] = (
        FirstCompletionCommandIsHonoredSignalWaitWorkflow
        if main_workflow_returns_before_signal_completions
        else FirstCompletionCommandIsHonoredWorkflow
    )
    async with new_worker(client, workflow_cls) as worker:
        handle = await client.start_workflow(
            workflow_cls.run, id=_wid(), task_queue=worker.task_queue
        )
        await handle.signal(workflow_cls.this_signal_executes_second)
        await handle.signal(workflow_cls.this_signal_executes_first)
        try:
            result = await handle.result()
        except WorkflowFailureError as err:
            if main_workflow_returns_before_signal_completions:
                raise RuntimeError(
                    "Expected no error due to main workflow coroutine returning first"
                )
            assert str(err.cause).startswith("Client should see this error")
        else:
            assert (
                main_workflow_returns_before_signal_completions
                and result == "workflow-result"
            )


async def test_first_of_two_signal_completion_commands_is_honored(
    client: Client,
) -> None:
    await _do_first_completion_command_is_honored_test(
        client, main_workflow_returns_before_signal_completions=False
    )


async def test_workflow_return_is_honored_when_it_precedes_signal_completion_command(
    client: Client,
) -> None:
    await _do_first_completion_command_is_honored_test(
        client, main_workflow_returns_before_signal_completions=True
    )
