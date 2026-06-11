"""ActivityEnvironment: pure in-memory activity testing, no database."""

import asyncio
from typing import Any, List

from temporal_dbos import activity
from temporal_dbos.testing import ActivityEnvironment


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
