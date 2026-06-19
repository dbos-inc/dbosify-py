"""External (async) completion of a cross-queue activity (§6.1.2).

``raise_complete_async()`` on the queued path parks the ``__temporal_activity``
workflow on its completion topic (rather than the parent run's inbox), addressed
by the task token. An external completer — here the test process, using the token
the activity handed off — completes it, and the workflow on its own worker
returns the delivered result.
"""

import asyncio
from pathlib import Path

import pytest
from dbos import DBOSClient

from dbosify.client import Client
from tests.dbconfig import system_database_url
from tests.harness import PythonProcess

WORKER = Path(__file__).parent / "cross_queue_async_worker.py"
REPO_ROOT = Path(__file__).parents[2]


def _env(vmid: str, token: "Path | None" = None, **extra: str) -> "dict[str, str]":
    env = {
        "PYTHONPATH": str(REPO_ROOT),
        "DBOSIFY_TEST_SYSTEM_DATABASE_URL": system_database_url(),
        "DBOS__VMID": vmid,
        **extra,
    }
    if token is not None:
        env["DBOSIFY_TEST_TOKEN"] = str(token)
    return env


async def _complete(
    task_token: bytes, value: str, *, heartbeat_first: bool = False
) -> None:
    dbos_client = DBOSClient(system_database_url=system_database_url())
    try:
        client = Client(dbos_client)
        handle = client.get_async_activity_handle(task_token=task_token)
        if heartbeat_first:
            # A heartbeat must NOT be mistaken for the completion.
            await handle.heartbeat("still working")
        await handle.complete(value)
    finally:
        dbos_client.destroy()


@pytest.mark.timeout(150)
@pytest.mark.usefixtures("cleanup_test_databases")
def test_queued_async_completion(tmp_path: Path) -> None:
    token_file = tmp_path / "token"
    activity_worker = PythonProcess(
        WORKER, "activity", env=_env("dbosify-act", token_file)
    )
    activity_worker.start()
    try:
        activity_worker.wait_for_line("ACTIVITY_WORKER_READY", timeout=90)
        workflow_worker = PythonProcess(
            WORKER,
            "workflow",
            "start",
            "xq-async-wf",
            env=_env("dbosify-wf", token_file),
        )
        workflow_worker.start()
        try:
            # The activity parked and handed off its task token.
            activity_worker.wait_for_line("ACTIVITY_PARKED", timeout=90)
            token = token_file.read_bytes()
            assert token, "activity did not write a task token"

            asyncio.run(_complete(token, "Hello, Temporal!"))

            line = workflow_worker.wait_for_line("RESULT ", timeout=90)
            assert workflow_worker.wait(timeout=30) == 0
            assert line.split("RESULT ", 1)[1].strip() == "Hello, Temporal!"
        finally:
            workflow_worker.terminate_and_wait()
    finally:
        activity_worker.terminate_and_wait()


@pytest.mark.timeout(150)
@pytest.mark.usefixtures("cleanup_test_databases")
def test_queued_async_heartbeat_then_complete(tmp_path: Path) -> None:
    # A heartbeat to a parked queued activity (it shares the completion topic)
    # is skipped, not mistaken for completion; the later complete() resolves it.
    token_file = tmp_path / "token"
    activity_worker = PythonProcess(
        WORKER, "activity", env=_env("dbosify-act", token_file)
    )
    activity_worker.start()
    try:
        activity_worker.wait_for_line("ACTIVITY_WORKER_READY", timeout=90)
        workflow_worker = PythonProcess(
            WORKER, "workflow", "start", "xq-async-hb-wf", env=_env("dbosify-wf")
        )
        workflow_worker.start()
        try:
            activity_worker.wait_for_line("ACTIVITY_PARKED", timeout=90)
            token = token_file.read_bytes()
            assert token, "activity did not write a task token"

            asyncio.run(_complete(token, "Hello, Temporal!", heartbeat_first=True))

            line = workflow_worker.wait_for_line("RESULT ", timeout=90)
            assert workflow_worker.wait(timeout=30) == 0
            assert line.split("RESULT ", 1)[1].strip() == "Hello, Temporal!"
        finally:
            workflow_worker.terminate_and_wait()
    finally:
        activity_worker.terminate_and_wait()


@pytest.mark.timeout(120)
@pytest.mark.usefixtures("cleanup_test_databases")
def test_queued_async_cancel_while_parked(tmp_path: Path) -> None:
    # Cancelling a queued activity that has async-parked reaches it promptly (a
    # marker wakes its completion-topic recv), yielding a real cancellation.
    activity_worker = PythonProcess(WORKER, "activity", env=_env("dbosify-act"))
    activity_worker.start()
    try:
        activity_worker.wait_for_line("ACTIVITY_WORKER_READY", timeout=90)
        workflow_worker = PythonProcess(
            WORKER,
            "workflow",
            "start",
            "xq-async-cancel-wf",
            env=_env("dbosify-wf", DBOSIFY_TEST_WF="cancel"),
        )
        workflow_worker.start()
        try:
            activity_worker.wait_for_line("ACTIVITY_PARKED", timeout=90)
            line = workflow_worker.wait_for_line("RESULT ", timeout=60)
            assert workflow_worker.wait(timeout=30) == 0
            assert line.split("RESULT ", 1)[1].strip() == "activity-cancelled"
        finally:
            workflow_worker.terminate_and_wait()
    finally:
        activity_worker.terminate_and_wait()
