"""A separate-process client against a versioned worker (DEVIATIONS D29).

A bare client (the test process — it never set this build id) enqueues a
workflow with no version (NULL); the versioned worker dequeues it (NULL matches
any version) and stamps its own build id at dequeue. So client-started workflows
are not pinned at enqueue but take the worker's build id when it picks them up —
a separate-process client with a different/absent version is not a sharp edge.
"""

import asyncio
from pathlib import Path
from typing import Optional, Tuple

import pytest
from dbos import DBOSClient

from dbosify.client import Client
from tests.dbconfig import system_database_url
from tests.harness import PythonProcess, build_id_env
from tests.integration.version_client_worker import TASK_QUEUE, VersionEcho

WORKER = Path(__file__).parent / "version_client_worker.py"


async def _client_run(wf_id: str) -> Tuple[str, Optional[str]]:
    # This process is a bare client: a DBOSClient, no launched DBOS, no build id
    # of its own. The enqueue carries no version; the worker stamps its own.
    dbos_client = DBOSClient(system_database_url=system_database_url())
    try:
        client = Client(dbos_client)
        result: str = await client.execute_workflow(
            VersionEcho.run, "hi", id=wf_id, task_queue=TASK_QUEUE
        )
        status = dbos_client.retrieve_workflow(wf_id).get_status()
        return result, status.app_version
    finally:
        dbos_client.destroy()


@pytest.mark.usefixtures("cleanup_test_databases")
def test_separate_process_client_workflow_takes_worker_build_id() -> None:
    worker = PythonProcess(WORKER, env=build_id_env("cli-build"))
    worker.start()
    try:
        worker.wait_for_line("READY", timeout=90)
        result, app_version = asyncio.run(_client_run("version-client-wf"))
    finally:
        worker.terminate_and_wait()

    assert result == "hi"
    # The worker stamped its build id on the NULL-versioned, client-enqueued run.
    assert app_version == "cli-build"
