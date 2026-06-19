"""ActivityEnvironment: pure in-memory activity testing, no database."""

import asyncio
from typing import Any, List

import pytest

from dbosify import activity
from dbosify.common import Priority
from dbosify.testing import ActivityEnvironment


@activity.defn
def sync_activity(name: str) -> str:
    activity.heartbeat("hb", 1)
    return f"{name}:{activity.info().activity_type}:{activity.info().attempt}"


@activity.defn
async def async_activity(name: str) -> str:
    activity.heartbeat("async-hb")
    return f"{name}:{activity.info().workflow_id}"


@activity.defn
def cancellable_activity() -> str:
    activity.wait_for_cancelled_sync(timeout=5)
    return "cancelled" if activity.is_cancelled() else "not cancelled"


@activity.defn
def worker_lifecycle_activity() -> str:
    # shield_thread_cancel_exception is a no-op here (cooperative cancellation,
    # DEVIATIONS sync-activity-cancel) — the body still runs.
    with activity.shield_thread_cancel_exception():
        shutdown = activity.is_worker_shutdown()
    activity.wait_for_worker_shutdown_sync(timeout=0.01)
    return f"shutdown={shutdown}"


@activity.defn
def client_activity() -> object:
    return activity.client()


def test_sync_activity_with_heartbeat() -> None:
    env = ActivityEnvironment()
    beats: List[Any] = []
    env.on_heartbeat = lambda *details: beats.append(details)
    assert env.run(sync_activity, "a") == "a:unknown:1"
    assert beats == [("hb", 1)]
    assert not activity.in_activity()  # context is reset after run


def test_async_activity() -> None:
    env = ActivityEnvironment()
    coro = env.run(async_activity, "b")
    assert asyncio.iscoroutine(coro)
    assert asyncio.run(coro) == "b:test"


def test_cancellation() -> None:
    env = ActivityEnvironment()
    env.cancel()
    assert env.run(cancellable_activity) == "cancelled"


def test_worker_lifecycle_helpers_without_worker() -> None:
    # ActivityEnvironment gives each activity its own (unset) worker-shutdown
    # event, so is_worker_shutdown() reads False with no manual reset needed.
    env = ActivityEnvironment()
    assert env.run(worker_lifecycle_activity) == "shutdown=False"


def test_environment_worker_shutdown_trips_flag() -> None:
    # ActivityEnvironment.worker_shutdown() trips the env's event, so
    # is_worker_shutdown() reads True and wait_for_worker_shutdown_sync returns.
    env = ActivityEnvironment()
    env.worker_shutdown()
    assert env.run(worker_lifecycle_activity) == "shutdown=True"


def test_client_helper_returns_environment_client() -> None:
    sentinel = object()
    env = ActivityEnvironment(client=sentinel)
    assert env.run(client_activity) is sentinel


def test_client_helper_raises_when_unavailable() -> None:
    # An ActivityEnvironment with no client has no client and no worker_state, so
    # activity.client() raises regardless of any worker in the process.
    env = ActivityEnvironment()
    with pytest.raises(RuntimeError, match="No client available"):
        env.run(client_activity)


def test_is_worker_shutdown_requires_activity_context() -> None:
    with pytest.raises(RuntimeError, match="Not in activity context"):
        activity.is_worker_shutdown()


def test_worker_states_have_independent_latched_events() -> None:
    # Each Worker gets its own _ActivityWorkerState with a fresh, latched shutdown
    # event: tripping one never affects another, and a bound context reads True.
    s1 = activity._ActivityWorkerState({"name": "app"})
    s2 = activity._ActivityWorkerState({"name": "app"})
    assert not s1.shutdown_event.is_set()
    assert not s2.shutdown_event.is_set()

    s1.shutdown_event.set()
    assert s1.shutdown_event.is_set()
    assert not s2.shutdown_event.is_set()  # per-worker isolation

    ctx = activity._Context(
        info=activity.Info(), on_heartbeat=lambda *a: None, worker_state=s1
    )
    token = activity._current_context.set(ctx)
    try:
        assert activity.is_worker_shutdown() is True
    finally:
        activity._current_context.reset(token)


def test_default_info_parity_fields() -> None:
    """The parity-ledger fields carry their honest defaults on a
    default-constructed Info (single-namespace, FIFO/no-priority)."""
    info = ActivityEnvironment().info
    assert info.namespace == "default"
    assert info.workflow_namespace == "default"
    assert info.priority is Priority.default
    assert info.activity_run_id is None
    # Timeouts/retry are unset until carried in from a real schedule.
    assert info.start_to_close_timeout is None
    assert info.schedule_to_close_timeout is None
    assert info.retry_policy is None
