"""activity.client() in a real worker (audit item 4): an async activity gets a
Temporal client (lazily built from the Worker's DBOS configuration) and uses it
to reach back into the system — here, to describe its own still-running
workflow.
"""

from contextlib import asynccontextmanager
from datetime import timedelta
from typing import AsyncIterator

import pytest
from dbos import DBOSClient

from dbosify import activity, workflow
from dbosify.client import Client
from dbosify.worker import Worker
from tests.dbconfig import default_config, system_database_url

pytestmark = pytest.mark.usefixtures("dbosify_env")

TASK_QUEUE = "activity-client-tq"


@activity.defn
async def describe_own_workflow() -> str:
    client = activity.client()
    handle = client.get_workflow_handle(activity.info().workflow_id)
    description = await handle.describe()
    assert description.status is not None
    return str(description.status.name)


@workflow.defn
class UsesActivityClient:
    @workflow.run
    async def run(self) -> str:
        status: str = await workflow.execute_activity(
            describe_own_workflow, start_to_close_timeout=timedelta(seconds=10)
        )
        return status


@asynccontextmanager
async def _env() -> AsyncIterator[Client]:
    worker = Worker(
        default_config(),
        task_queue=TASK_QUEUE,
        workflows=[UsesActivityClient],
        activities=[describe_own_workflow],
    )
    async with worker:
        dbos_client = DBOSClient(system_database_url=system_database_url())
        try:
            yield Client(dbos_client)
        finally:
            dbos_client.destroy()


async def test_activity_client_describes_own_workflow() -> None:
    async with _env() as client:
        result = await client.execute_workflow(
            UsesActivityClient.run, id="uses-activity-client", task_queue=TASK_QUEUE
        )
    # The parent was RUNNING while the activity described it through its client.
    assert result == "RUNNING"
