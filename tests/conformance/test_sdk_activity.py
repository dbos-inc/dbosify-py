"""Conformance: activity behavioral tests adapted from temporalio's own SDK
suite (``tests/worker/test_activity.py`` in temporal-sdk-python).

temporalio drives these through a server-side ``kitchen_sink`` workflow plus an
``ExternalWorker``. We replace that driver with a small dbosify workflow that
calls ``workflow.execute_activity(...)`` and surfaces failures as
``WorkflowFailureError.cause`` (an ``ActivityError`` whose ``.cause`` is the
converted ``ApplicationError``) — exactly temporalio's ``assert_activity_*``
shape. Sync-executor, multiprocess, server-history, and cancellation-detail
variants are left in the SDK suite (documented non-goals / deviations).
"""

import uuid
from dataclasses import dataclass
from datetime import timedelta
from typing import Any, Callable, Dict, List, Optional, Sequence

import pytest

from dbosify import activity, workflow
from dbosify.client import Client, WorkflowFailureError
from dbosify.common import RawValue, RetryPolicy
from dbosify.exceptions import ActivityError, ApplicationError

from .sdk_harness import new_worker, wid

# ---------------------------------------------------------------------------
# Activities under test (module-level so the Worker can register them)
# ---------------------------------------------------------------------------


@activity.defn
async def say_hello(name: str) -> str:
    return f"Hello, {name}!"


@activity.defn(name="my custom activity name!")
async def get_name(_name: str) -> str:
    return f"Name: {activity.info().activity_type}"


@activity.defn
async def capture_info() -> Dict[str, Any]:
    info = activity.info()
    return {
        "has_activity_id": bool(info.activity_id),
        "activity_type": info.activity_type,
        "attempt": info.attempt,
        "is_local": info.is_local,
        "task_queue": info.task_queue,
        "workflow_id": info.workflow_id,
        "workflow_type": info.workflow_type,
        "has_task_token": bool(info.task_token),
        "start_to_close": (
            info.start_to_close_timeout.total_seconds()
            if info.start_to_close_timeout
            else None
        ),
        "heartbeat_details": list(info.heartbeat_details),
        "heartbeat_timeout": (
            info.heartbeat_timeout.total_seconds() if info.heartbeat_timeout else None
        ),
        "schedule_to_close": (
            info.schedule_to_close_timeout.total_seconds()
            if info.schedule_to_close_timeout
            else None
        ),
    }


@activity.defn
async def raise_error() -> None:
    raise RuntimeError("oh no!")


@activity.defn
async def heartbeating_activity() -> str:
    info = activity.info()
    count = int(next(iter(info.heartbeat_details))) if info.heartbeat_details else 0
    count += 9
    activity.heartbeat(count)
    if count < 30:
        raise RuntimeError("Try again!")
    return f"final count: {count}"


@activity.defn
async def non_retryable_activity() -> None:
    if activity.info().attempt < 2:
        raise ApplicationError("Retry me", non_retryable=False)
    raise ApplicationError("Do not retry me", "detail1", 123, non_retryable=True)


@activity.defn
async def non_retryable_type_activity() -> None:
    if activity.info().attempt < 2:
        raise ApplicationError("Retry me", type="Can retry me")
    raise ApplicationError("Do not retry me", type="Cannot retry me")


@dataclass
class SomeClass1:
    foo: int


@dataclass
class SomeClass2:
    foo: str
    bar: Optional[SomeClass1] = None


@activity.defn
async def type_hints_activity(param1: SomeClass2, param2: str) -> str:
    return (
        f"param1: {type(param1).__name__}({param1.foo},{param1.bar}), "
        f"param2: {type(param2).__name__}"
    )


@dataclass
class DynActivityValue:
    some_field: str


@activity.defn(dynamic=True)
async def dynamic_activity(args: Sequence[RawValue]) -> DynActivityValue:
    conv = activity.payload_converter()
    values = [conv.from_payload(a.payload, DynActivityValue) for a in args]
    name = activity.info().activity_type
    joined = " - ".join(v.some_field for v in values)
    return DynActivityValue(f"{name} - {joined}")


_ACTIVITIES: List[Callable[..., Any]] = [
    say_hello,
    get_name,
    capture_info,
    raise_error,
    heartbeating_activity,
    non_retryable_activity,
    non_retryable_type_activity,
    type_hints_activity,
]

# ---------------------------------------------------------------------------
# Driver workflows
# ---------------------------------------------------------------------------


def _call(name: str, **opts: Any) -> Dict[str, Any]:
    return {"name": name, **opts}


@workflow.defn
class RunActivity:
    """Executes one registered activity by name and returns its result (or lets
    its failure propagate as a WorkflowFailureError)."""

    @workflow.run
    async def run(self, call: Dict[str, Any]) -> Any:
        kwargs: Dict[str, Any] = {"args": list(call.get("args", []))}
        start_to_close = call.get("start_to_close", 10.0)
        if start_to_close is not None:
            kwargs["start_to_close_timeout"] = timedelta(seconds=start_to_close)
        if call.get("heartbeat_timeout") is not None:
            kwargs["heartbeat_timeout"] = timedelta(seconds=call["heartbeat_timeout"])
        # Default to a single attempt (as temporalio's kitchen_sink driver does);
        # without this a raising activity retries forever and the test hangs.
        non_retryable = call.get("non_retryable_types")
        kwargs["retry_policy"] = RetryPolicy(
            maximum_attempts=call.get("max_attempts", 1),
            initial_interval=timedelta(milliseconds=10),
            backoff_coefficient=1.0,
            non_retryable_error_types=list(non_retryable) if non_retryable else None,
        )
        return await workflow.execute_activity(call["name"], **kwargs)


@workflow.defn
class RunTypeHints:
    @workflow.run
    async def run(self, param1: SomeClass2, param2: str) -> str:
        result: str = await workflow.execute_activity(
            type_hints_activity,
            args=[param1, param2],
            start_to_close_timeout=timedelta(seconds=10),
        )
        return result


@workflow.defn
class RunDynamic:
    @workflow.run
    async def run(self, a: DynActivityValue, b: DynActivityValue) -> DynActivityValue:
        result: DynActivityValue = await workflow.execute_activity(
            "some-activity-name",
            args=[a, b],
            result_type=DynActivityValue,
            start_to_close_timeout=timedelta(seconds=10),
        )
        return result


# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------


async def test_activity_hello(client: Client) -> None:
    async with new_worker(client, RunActivity, activities=_ACTIVITIES) as worker:
        result = await client.execute_workflow(
            RunActivity.run,
            _call("say_hello", args=["Temporal"]),
            id=wid(),
            task_queue=worker.task_queue,
        )
    assert result == "Hello, Temporal!"


async def test_activity_custom_name(client: Client) -> None:
    async with new_worker(client, RunActivity, activities=_ACTIVITIES) as worker:
        result = await client.execute_workflow(
            RunActivity.run,
            _call("my custom activity name!", args=["Temporal"]),
            id=wid(),
            task_queue=worker.task_queue,
        )
    assert result == "Name: my custom activity name!"


async def test_activity_info(client: Client) -> None:
    async with new_worker(client, RunActivity, activities=_ACTIVITIES) as worker:
        info = await client.execute_workflow(
            RunActivity.run,
            _call("capture_info", start_to_close=4.0),
            id=wid(),
            task_queue=worker.task_queue,
        )
        tq = worker.task_queue
    assert info["has_activity_id"] is True
    assert info["activity_type"] == "capture_info"
    assert info["attempt"] == 1
    assert info["is_local"] is False
    assert info["task_queue"] == tq
    assert info["workflow_type"] == "RunActivity"
    assert info["has_task_token"] is True
    assert info["start_to_close"] == 4.0
    assert info["heartbeat_details"] == []
    assert info["heartbeat_timeout"] is None
    assert info["schedule_to_close"] is None


async def test_activity_failure(client: Client) -> None:
    async with new_worker(client, RunActivity, activities=_ACTIVITIES) as worker:
        with pytest.raises(WorkflowFailureError) as err:
            await client.execute_workflow(
                RunActivity.run,
                _call("raise_error"),
                id=wid(),
                task_queue=worker.task_queue,
            )
    app_err = _app_error(err.value)
    assert app_err.message == "oh no!"
    assert app_err.type == "RuntimeError"


async def test_activity_bad_params(client: Client) -> None:
    # say_hello needs one positional arg; calling with none surfaces the
    # binding TypeError as an activity ApplicationError.
    async with new_worker(client, RunActivity, activities=_ACTIVITIES) as worker:
        with pytest.raises(WorkflowFailureError) as err:
            await client.execute_workflow(
                RunActivity.run,
                _call("say_hello"),
                id=wid(),
                task_queue=worker.task_queue,
            )
    assert "missing 1 required positional argument: 'name'" in _app_error(
        err.value
    ).message


async def test_activity_type_hints(client: Client) -> None:
    async with new_worker(client, RunTypeHints, activities=_ACTIVITIES) as worker:
        result = await client.execute_workflow(
            RunTypeHints.run,
            args=[SomeClass2(foo="str1", bar=SomeClass1(foo=123)), "123"],
            id=wid(),
            task_queue=worker.task_queue,
        )
    # The activity received a reconstructed SomeClass2 (incl. nested SomeClass1)
    # and a str, not raw dicts.
    assert result == "param1: SomeClass2(str1,SomeClass1(foo=123)), param2: str"


async def test_activity_heartbeat_details(client: Client) -> None:
    # Heartbeat details from a failed attempt are visible to the next attempt;
    # the activity accumulates to 36 over four attempts and then succeeds.
    async with new_worker(client, RunActivity, activities=_ACTIVITIES) as worker:
        result = await client.execute_workflow(
            RunActivity.run,
            _call("heartbeating_activity", max_attempts=4),
            id=wid(),
            task_queue=worker.task_queue,
        )
    assert result == "final count: 36"


async def test_activity_error_non_retryable(client: Client) -> None:
    async with new_worker(client, RunActivity, activities=_ACTIVITIES) as worker:
        with pytest.raises(WorkflowFailureError) as err:
            await client.execute_workflow(
                RunActivity.run,
                _call("non_retryable_activity", max_attempts=100),
                id=wid(),
                task_queue=worker.task_queue,
            )
    app_err = _app_error(err.value)
    assert app_err.message == "Do not retry me"
    assert list(app_err.details) == ["detail1", 123]


async def test_activity_error_non_retryable_type(client: Client) -> None:
    async with new_worker(client, RunActivity, activities=_ACTIVITIES) as worker:
        with pytest.raises(WorkflowFailureError) as err:
            await client.execute_workflow(
                RunActivity.run,
                _call(
                    "non_retryable_type_activity",
                    max_attempts=100,
                    non_retryable_types=["Cannot retry me"],
                ),
                id=wid(),
                task_queue=worker.task_queue,
            )
    app_err = _app_error(err.value)
    assert app_err.message == "Do not retry me"
    assert app_err.type == "Cannot retry me"


async def test_activity_dynamic(client: Client) -> None:
    async with new_worker(client, RunDynamic, activities=[dynamic_activity]) as worker:
        result = await client.execute_workflow(
            RunDynamic.run,
            args=[DynActivityValue("val1"), DynActivityValue("val2")],
            id=wid(),
            task_queue=worker.task_queue,
        )
    assert result == DynActivityValue("some-activity-name - val1 - val2")


async def test_activity_dynamic_duplicate(client: Client) -> None:
    # Registering two dynamic activities is rejected at Worker construction.
    with pytest.raises(TypeError, match="More than one dynamic activity"):
        async with new_worker(
            client, RunDynamic, activities=[dynamic_activity, _other_dynamic]
        ):
            pass


@activity.defn(dynamic=True)
async def _other_dynamic(_args: Sequence[RawValue]) -> None:
    pass


def _app_error(err: WorkflowFailureError) -> ApplicationError:
    assert isinstance(err.cause, ActivityError)
    assert isinstance(err.cause.cause, ApplicationError)
    return err.cause.cause
