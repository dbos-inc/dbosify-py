"""Namespaces backed by DBOS schemas (DEVIATIONS no-server): a Worker/Client pair runs
in a namespace's own schema, and clients for different namespaces coexist in one
process (per-engine schema isolation). The low-level constructor's rejection of
a non-namespace schema is unit-tested in tests/unit/test_namespaces.py."""

import pytest
from dbos import DBOSConfig

from dbosify import worker as _worker_mod
from dbosify import workflow
from dbosify.client import Client
from dbosify.worker import Worker
from tests.dbconfig import system_database_url

pytestmark = pytest.mark.usefixtures("dbosify_env")

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
        "name": f"dbosify_ns_{namespace}",
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


async def test_clients_for_different_namespaces_coexist() -> None:
    # Run one workflow id in two namespaces, then read both back through two live
    # clients: each engine isolates its schema, so neither client clobbers the other.
    wf_id = "shared-wf"
    for namespace in ("alpha", "beta"):
        worker = Worker(
            _config_for(namespace),
            task_queue=TASK_QUEUE,
            namespace=namespace,
            workflows=[NamespaceWorkflow],
        )
        async with worker:
            async with await Client.connect(
                system_database_url(), namespace=namespace
            ) as client:
                assert (
                    await client.execute_workflow(
                        NamespaceWorkflow.run, id=wf_id, task_queue=TASK_QUEUE
                    )
                    == namespace
                )
        _worker_mod._reset_for_tests()

    client_a = await Client.connect(system_database_url(), namespace="alpha")
    client_b = await Client.connect(system_database_url(), namespace="beta")
    try:
        # The same id resolved through each client yields that namespace's run.
        assert await client_a.get_workflow_handle(wf_id).result() == "alpha"
        assert await client_b.get_workflow_handle(wf_id).result() == "beta"
        # Re-read alpha while beta's client is also live — no clobbering.
        assert await client_a.get_workflow_handle(wf_id).result() == "alpha"
    finally:
        await client_a.close()
        await client_b.close()
