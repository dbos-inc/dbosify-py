"""Namespaces backed by DBOS schemas (DEVIATIONS D1): a Worker/Client pair in
a non-default namespace runs in that namespace's own schema, and a Client whose
DBOSClient was built for a different schema is rejected."""

import pytest
from dbos import DBOSClient, DBOSConfig

from temporal_dbos import workflow
from temporal_dbos._internal.namespaces import namespace_schema
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
        dbos_client = DBOSClient(
            system_database_url=system_database_url(),
            dbos_system_schema=namespace_schema(namespace),
        )
        try:
            client = await Client.connect(dbos_client, namespace=namespace)
            assert client.namespace == namespace
            # The workflow runs in the alt schema and reports its namespace.
            result = await client.execute_workflow(
                NamespaceWorkflow.run, id="ns-wf", task_queue=TASK_QUEUE
            )
            assert result == namespace
        finally:
            dbos_client.destroy()


async def test_client_namespace_schema_mismatch_rejected() -> None:
    # A DBOSClient built for one namespace's schema cannot back a Client for a
    # different namespace — it would target the wrong schema (and, since the
    # DBOS SystemSchema is process-global, clobber a co-located Worker). A
    # Worker stands the database up first (DBOSClient connects at construction).
    worker = Worker(
        _config_for("default"),
        task_queue=TASK_QUEUE,
        namespace="default",
        workflows=[NamespaceWorkflow],
    )
    async with worker:
        dbos_client = DBOSClient(
            system_database_url=system_database_url(),
            dbos_system_schema=namespace_schema("default"),
        )
        try:
            with pytest.raises(ValueError, match="does not match namespace"):
                await Client.connect(dbos_client, namespace="alt")
        finally:
            dbos_client.destroy()
