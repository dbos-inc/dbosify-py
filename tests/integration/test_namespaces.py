"""Namespaces backed by DBOS schemas (DEVIATIONS D1): a Worker/Client pair in a
non-default namespace runs in that namespace's own schema. (The low-level
constructor's rejection of a non-namespace schema is unit-tested in
tests/unit/test_namespaces.py — building such a DBOSClient here would trip the
pre-#728 process-global-schema clobbering, not exercise our code.)"""

import pytest
from dbos import DBOSConfig

from temporal_dbos import workflow
from temporal_dbos.client import Client
from temporal_dbos.worker import Worker
from tests.dbconfig import system_database_url

pytestmark = pytest.mark.usefixtures("tdb_env")

TASK_QUEUE = "ns-tq"


@workflow.defn
class NamespaceWorkflow:
    @workflow.run
    async def run(self) -> str:
        return workflow.info().namespace


def _config_for(namespace: str) -> DBOSConfig:
    # No dbos_system_schema: the Worker derives it from `namespace` (the Worker
    # also installs the serializer). A conflicting explicit schema is an error.
    return {
        "name": f"tdb_ns_{namespace}",
        "system_database_url": system_database_url(),
        "run_admin_server": False,
        "notification_listener_polling_interval_sec": 0.01,
    }


async def test_workflow_runs_in_named_namespace() -> None:
    namespace = "alt"
    worker = Worker(
        _config_for(namespace),
        task_queue=TASK_QUEUE,
        namespace=namespace,
        workflows=[NamespaceWorkflow],
    )
    async with worker:
        assert worker.namespace == namespace
        # connect builds the DBOSClient for this namespace's schema — the
        # namespace is stated once, no dbos_system_schema in sight.
        async with await Client.connect(
            system_database_url(), namespace=namespace
        ) as client:
            assert client.namespace == namespace
            # The workflow runs in the alt schema and reports its namespace.
            result = await client.execute_workflow(
                NamespaceWorkflow.run, id="ns-wf", task_queue=TASK_QUEUE
            )
            assert result == namespace
