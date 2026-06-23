"""Shared harness for the adapted temporalio SDK conformance suites.

The ``client`` fixture lives in ``tests/conformance/conftest.py`` (so every
``test_sdk_*.py`` module in this directory gets it automatically). This module
holds the rest: the ``new_worker`` adapter, the ``assert_eq_eventually`` polling
helper, a unique-id helper, and a couple of trivially shared workflow/activity
definitions.
"""

import asyncio
import time
import uuid
from contextlib import asynccontextmanager
from datetime import timedelta
from typing import Any, AsyncIterator, Awaitable, Callable, Sequence, TypeVar

from dbosify import activity, workflow
from dbosify.client import Client
from dbosify.converter import DataConverter
from dbosify.worker import Interceptor, Worker
from tests.dbconfig import default_config

T = TypeVar("T")


@asynccontextmanager
async def new_worker(
    client: Client,
    *workflows: type,
    activities: Sequence[Callable[..., Any]] = (),
    task_queue: str | None = None,
    workflow_failure_exception_types: Sequence[type[BaseException]] = (),
    data_converter: DataConverter = DataConverter.default,
    interceptors: Sequence[Interceptor] = (),
    **_ignored: Any,
) -> AsyncIterator[Worker]:
    """temporalio's ``new_worker(client, *workflows, activities=...)`` over our
    ``Worker(DBOSConfig, ...)``. The Worker owns the DBOS lifecycle; the passed
    ``client`` already targets the same database.

    ``data_converter``/``interceptors`` are forwarded to the Worker. The
    converter is process-global, so configuring it on the Worker
    also applies to the ``client`` for the duration of the test."""
    worker = Worker(
        default_config(),
        task_queue=task_queue or f"sdk-tq-{uuid.uuid4()}",
        workflows=list(workflows),
        activities=list(activities),
        workflow_failure_exception_types=list(workflow_failure_exception_types),
        data_converter=data_converter,
        interceptors=list(interceptors),
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


def wid() -> str:
    return f"workflow-{uuid.uuid4()}"


@activity.defn
async def say_hello(name: str) -> str:
    return f"Hello, {name}!"


@workflow.defn
class HelloWorkflow:
    @workflow.run
    async def run(self, name: str) -> str:
        return f"Hello, {name}!"


@workflow.defn
class _SchemaWarmupWorkflow:
    @workflow.run
    async def run(self) -> None:
        pass


async def warm_schema(client: Client) -> None:
    """Migrate the namespace schema before a client-before-worker test issues
    start/describe/update calls. Our Worker owns schema creation, so
    on a freshly-dropped test database the schema does not exist until a Worker
    launches; production always has it pre-migrated. Launching and immediately
    stopping a throwaway worker creates the schema (it persists), faithfully
    modelling the production precondition."""
    async with new_worker(
        client, _SchemaWarmupWorkflow, task_queue=f"warmup-{uuid.uuid4()}"
    ):
        pass
