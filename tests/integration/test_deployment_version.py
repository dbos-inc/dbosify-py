"""Worker deployment versioning (audit item 2), built on DBOS versioning
(DEVIATIONS D29): a build ID *is* the DBOS ``application_version`` (set via
``build_id`` / ``deployment_config``, else derived from the app name +
``application_version``), surfaced via
``workflow.Info.get_current_deployment_version()``. Because DBOS scopes recovery
and queue dequeue to ``application_version``, PINNED is the enforced default;
AUTO_UPGRADE has no analog. These accessors use the workflow runtime, so the
workflow returns them from ``run()`` rather than a query.
"""

from contextlib import asynccontextmanager
from typing import Any, AsyncIterator, Dict, Optional

import pytest
from dbos import DBOSClient

from temporal_dbos import workflow
from temporal_dbos.client import Client
from temporal_dbos.common import (
    VersioningBehavior,
    WorkerDeploymentVersion,
)
from temporal_dbos.worker import DEFAULT_APP_VERSION, Worker, WorkerDeploymentConfig
from tests.dbconfig import default_config, system_database_url

pytestmark = pytest.mark.usefixtures("tdb_env")

TASK_QUEUE = "deployment-version-tq"
# default_config()["name"]
APP_NAME = "temporal_dbos_test"


@workflow.defn
class DeploymentInfoWorkflow:
    @workflow.run
    async def run(self) -> Dict[str, Any]:
        version = workflow.info().get_current_deployment_version()
        return {
            "deployment_name": version.deployment_name if version else None,
            "build_id": version.build_id if version else None,
            "build_id_method": workflow.info().get_current_build_id(),
            "target_changed": (
                workflow.info().is_target_worker_deployment_version_changed()
            ),
        }


@workflow.defn(versioning_behavior=VersioningBehavior.PINNED)
class PinnedWorkflow:
    @workflow.run
    async def run(self) -> str:
        # PINNED is what DBOS enforces by default (recovery/dequeue scoped to
        # application_version); the workflow runs normally.
        return "pinned-ok"


@workflow.defn(versioning_behavior=VersioningBehavior.AUTO_UPGRADE)
class AutoUpgradeWorkflow:
    @workflow.run
    async def run(self) -> str:
        # AUTO_UPGRADE has no DBOS analog and degrades to pinned; accepted, runs.
        return "auto-ok"


@workflow.defn
class CANOnceWorkflow:
    @workflow.run
    async def run(self, n: int) -> str:
        if n == 0:
            workflow.continue_as_new(1)
        return f"done-{n}"


_WORKFLOWS = [
    DeploymentInfoWorkflow,
    PinnedWorkflow,
    AutoUpgradeWorkflow,
    CANOnceWorkflow,
]


@asynccontextmanager
async def _env(
    *,
    build_id: Optional[str] = None,
    deployment_config: Optional[WorkerDeploymentConfig] = None,
) -> AsyncIterator[Client]:
    worker = Worker(
        default_config(),
        task_queue=TASK_QUEUE,
        workflows=_WORKFLOWS,
        build_id=build_id,
        deployment_config=deployment_config,
    )
    async with worker:
        dbos_client = DBOSClient(system_database_url=system_database_url())
        try:
            yield await Client.connect(dbos_client)
        finally:
            dbos_client.destroy()


async def test_explicit_build_id() -> None:
    async with _env(build_id="bld-xyz") as client:
        result = await client.execute_workflow(
            DeploymentInfoWorkflow.run, id="dv-build-id", task_queue=TASK_QUEUE
        )
    assert result["deployment_name"] == APP_NAME
    assert result["build_id"] == "bld-xyz"
    assert result["build_id_method"] == "bld-xyz"
    assert result["target_changed"] is False
    # The build_id IS the DBOS application_version — the version DBOS scopes
    # recovery and queue dequeue to. That equality is what makes the reported
    # deployment version the *actual* pinned routing version (PINNED is real,
    # not cosmetic). See DEVIATIONS D29.
    probe = DBOSClient(system_database_url=system_database_url())
    try:
        status = probe.retrieve_workflow("dv-build-id").get_status()
    finally:
        probe.destroy()
    assert status.app_version == "bld-xyz"


async def test_default_build_id_from_application_version() -> None:
    async with _env() as client:
        result = await client.execute_workflow(
            DeploymentInfoWorkflow.run, id="dv-default", task_queue=TASK_QUEUE
        )
    # No explicit build_id: derived from the DBOS application name +
    # application_version (DEFAULT_APP_VERSION pinned by the Worker).
    assert result["deployment_name"] == APP_NAME
    assert result["build_id"] == "0.1"


async def test_deployment_config() -> None:
    config = WorkerDeploymentConfig(
        version=WorkerDeploymentVersion(deployment_name="my-dep", build_id="42"),
        use_worker_versioning=True,
        default_versioning_behavior=VersioningBehavior.AUTO_UPGRADE,
    )
    async with _env(deployment_config=config) as client:
        result = await client.execute_workflow(
            DeploymentInfoWorkflow.run, id="dv-config", task_queue=TASK_QUEUE
        )
    assert result["deployment_name"] == "my-dep"
    assert result["build_id"] == "42"


async def test_pinned_versioning_behavior_runs() -> None:
    # versioning_behavior=PINNED is exactly what DBOS enforces by default
    # (recovery/dequeue scoped to application_version); the workflow runs.
    async with _env() as client:
        result = await client.execute_workflow(
            PinnedWorkflow.run, id="dv-pinned", task_queue=TASK_QUEUE
        )
    assert result == "pinned-ok"


async def test_auto_upgrade_behavior_accepted_and_degrades_to_pinned() -> None:
    # AUTO_UPGRADE has no DBOS analog; it is accepted and the workflow runs
    # (pinned), rather than erroring.
    async with _env() as client:
        result = await client.execute_workflow(
            AutoUpgradeWorkflow.run, id="dv-auto", task_queue=TASK_QUEUE
        )
    assert result == "auto-ok"


async def test_continue_as_new_successor_inherits_build_id() -> None:
    # A continue-as-new run is a fresh workflow enqueued from inside the worker,
    # so DBOS stamps it with the worker's application_version (= build_id): the
    # successor run is pinned to the same build ID as its predecessor.
    async with _env(build_id="can-build") as client:
        result = await client.execute_workflow(
            CANOnceWorkflow.run, 0, id="dv-can", task_queue=TASK_QUEUE
        )
    assert result == "done-1"
    probe = DBOSClient(system_database_url=system_database_url())
    try:
        # Run-chain id scheme (§6.4): successor run n=1 is "<id>--r1".
        successor = probe.retrieve_workflow("dv-can--r1").get_status()
    finally:
        probe.destroy()
    assert successor.app_version == "can-build"


async def test_auto_versioning_reports_computed_version_not_empty() -> None:
    # application_version=None opts into DBOS code-hash auto-versioning. The
    # reported build_id must be the live computed version DBOS pins on (read at
    # access time), not the empty construction-time value.
    config = default_config()
    config["application_version"] = None
    worker = Worker(config, task_queue=TASK_QUEUE, workflows=_WORKFLOWS)
    async with worker:
        dbos_client = DBOSClient(system_database_url=system_database_url())
        try:
            client = await Client.connect(dbos_client)
            result = await client.execute_workflow(
                DeploymentInfoWorkflow.run, id="dv-autover", task_queue=TASK_QUEUE
            )
        finally:
            dbos_client.destroy()
    assert result["build_id"], "auto-versioned build_id should be the computed hash"
    # Must be the *computed* code-hash, not a silent fallback to the pinned
    # default — otherwise this would pass even if auto-versioning regressed.
    assert result["build_id"] != DEFAULT_APP_VERSION
    probe = DBOSClient(system_database_url=system_database_url())
    try:
        status = probe.retrieve_workflow("dv-autover").get_status()
    finally:
        probe.destroy()
    # Reported version equals the version DBOS actually enforced on the run.
    assert result["build_id"] == status.app_version


def test_worker_rejects_build_id_and_deployment_config_together() -> None:
    config = WorkerDeploymentConfig(
        version=WorkerDeploymentVersion("d", "b"), use_worker_versioning=True
    )
    with pytest.raises(ValueError, match="build_id and deployment_config"):
        Worker(
            default_config(),
            task_queue=TASK_QUEUE,
            workflows=_WORKFLOWS,
            build_id="x",
            deployment_config=config,
        )


def test_worker_rejects_empty_build_id() -> None:
    with pytest.raises(ValueError, match="non-empty"):
        Worker(
            default_config(), task_queue=TASK_QUEUE, workflows=_WORKFLOWS, build_id=""
        )


def test_worker_rejects_build_id_conflicting_with_config_version() -> None:
    config = default_config()
    config["application_version"] = "cfg-ver"
    with pytest.raises(ValueError, match="conflicts with"):
        Worker(
            config, task_queue=TASK_QUEUE, workflows=_WORKFLOWS, build_id="bld-other"
        )


def test_worker_rejects_build_id_with_explicit_none_version() -> None:
    # application_version=None (auto-versioning opt-in) + build_id is
    # contradictory; the key-presence conflict check must catch the None case.
    config = default_config()
    config["application_version"] = None
    with pytest.raises(ValueError, match="conflicts with"):
        Worker(config, task_queue=TASK_QUEUE, workflows=_WORKFLOWS, build_id="bld")


def test_worker_rejects_use_worker_versioning_with_deployment_config() -> None:
    deployment_config = WorkerDeploymentConfig(
        version=WorkerDeploymentVersion("dep", "v1"), use_worker_versioning=True
    )
    with pytest.raises(ValueError, match="cannot be combined with deployment_config"):
        Worker(
            default_config(),
            task_queue=TASK_QUEUE,
            workflows=_WORKFLOWS,
            deployment_config=deployment_config,
            use_worker_versioning=True,
        )


def test_worker_rejects_use_worker_versioning_without_build_id() -> None:
    # Match the specific message — "use_worker_versioning" alone appears in both
    # versioning-validation errors, so it wouldn't pin this branch.
    with pytest.raises(ValueError, match="build_id must be specified"):
        Worker(
            default_config(),
            task_queue=TASK_QUEUE,
            workflows=_WORKFLOWS,
            use_worker_versioning=True,
        )
