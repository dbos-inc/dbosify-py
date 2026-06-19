"""workflow.info().root: the root workflow of a run's tree, threaded to
children (§6.6). None for a top-level workflow (itself the root)."""

from contextlib import asynccontextmanager
from typing import Any, AsyncIterator, Dict, Optional

import pytest
from dbos import DBOSClient

from dbosify import workflow
from dbosify.client import Client
from dbosify.worker import Worker
from tests.dbconfig import default_config, system_database_url

pytestmark = pytest.mark.usefixtures("dbosify_env")

TASK_QUEUE = "info-root-tq"


@workflow.defn
class RootChild:
    @workflow.run
    async def run(self) -> Dict[str, Optional[str]]:
        root = workflow.info().root
        return {
            "root_workflow_id": root.workflow_id if root else None,
            "root_run_id": root.run_id if root else None,
        }


@workflow.defn
class RootParent:
    @workflow.run
    async def run(self) -> Dict[str, Any]:
        return {
            "parent_root_is_none": workflow.info().root is None,
            "parent_id": workflow.info().workflow_id,
            "child": await workflow.execute_child_workflow(
                RootChild.run, id="info-root-child"
            ),
        }


@asynccontextmanager
async def _env() -> AsyncIterator[Client]:
    worker = Worker(
        default_config(),
        task_queue=TASK_QUEUE,
        workflows=[RootParent, RootChild],
    )
    async with worker:
        dbos_client = DBOSClient(system_database_url=system_database_url())
        try:
            yield Client(dbos_client)
        finally:
            dbos_client.destroy()


async def test_info_root_threaded_to_children() -> None:
    async with _env() as client:
        result = await client.execute_workflow(
            RootParent.run, id="info-root-parent", task_queue=TASK_QUEUE
        )
    # A top-level workflow is its own root → info().root is None.
    assert result["parent_root_is_none"] is True
    # The child's root points back at the (top-level) parent.
    assert result["child"]["root_workflow_id"] == result["parent_id"]
    assert result["child"]["root_run_id"] == "info-root-parent"
