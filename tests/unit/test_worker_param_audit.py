"""Accepted-parameter audit for ``Worker`` (DESIGN §9): every parameter of
``temporalio.worker.Worker.__init__`` is classified — honored, inert (accepted
but a defensible no-op), rejected (behavior-changing + unsupported → raises), or
a fundamental deviation. The completeness check is machine-enforced: a new
temporalio Worker parameter fails this test until it is classified, so no
behavior-changing option can be silently swallowed.
"""

import inspect
from typing import Any, Dict, Set, cast

import pytest
import temporalio.worker
from dbos import DBOSConfig

from temporal_dbos import workflow
from temporal_dbos.worker import _REJECTED_OPTIONS, Worker

# We map it onto a DBOS primitive (act on it).
HONORED: Set[str] = {
    "task_queue",
    "workflows",
    "activities",
    "activity_executor",
    "interceptors",
    "build_id",
    "identity",
    "max_concurrent_workflow_tasks",
    "max_concurrent_activities",
    "max_concurrent_local_activities",
    "max_activities_per_second",
    "max_task_queue_activities_per_second",
    "graceful_shutdown_timeout",
    "workflow_failure_exception_types",
    "on_fatal_error",
    "use_worker_versioning",
    "deployment_config",
}

# Behavior-changing AND unsupported → Worker raises if set (never silent).
REJECTED: Set[str] = set(_REJECTED_OPTIONS)

# Fundamentally different — the leading positional is a DBOSConfig, not a client.
DEVIATION: Set[str] = {"client"}

# Accepted but genuinely a no-op in our model — each with a defensible reason.
INERT: Dict[str, str] = {
    "workflow_task_executor": "interpreter runs on the event loop; no separate workflow-task thread pool",
    "nexus_task_executor": "Nexus not supported (DESIGN §1)",
    "workflow_runner": "no workflow sandbox (D13)",
    "unsandboxed_workflow_runner": "no workflow sandbox (D13)",
    "max_cached_workflows": "no sticky cache; workflows replay from DBOS checkpoints",
    "max_concurrent_nexus_tasks": "Nexus not supported (DESIGN §1)",
    "max_concurrent_workflow_task_polls": "DBOS queue listener, not Temporal long-polling",
    "nonsticky_to_sticky_poll_ratio": "no sticky cache",
    "max_concurrent_activity_task_polls": "DBOS queue listener, not Temporal long-polling",
    "no_remote_activities": "cross-queue dispatch model, not remote-activity polling",
    "sticky_queue_schedule_to_start_timeout": "no sticky cache",
    "max_heartbeat_throttle_interval": "in-memory heartbeat model (D6)",
    "default_heartbeat_throttle_interval": "in-memory heartbeat model (D6)",
    "shared_state_manager": "no multiprocess activities (D9)",
    "debug_mode": "no Temporal-style workflow-task deadlock detector",
    "disable_eager_activity_execution": "no server-side eager-activity optimization",
    "disable_safe_workflow_eviction": "no sticky-cache eviction",
    "workflow_task_poller_behavior": "DBOS queue listener, not Temporal pollers",
    "activity_task_poller_behavior": "DBOS queue listener, not Temporal pollers",
    "nexus_task_poller_behavior": "Nexus not supported (DESIGN §1)",
    "disable_payload_error_limit": "Temporal payload-size caps not enforced (D8/§6.9)",
    "max_workflow_task_external_storage_concurrency": "no external payload storage (§6.9)",
}


def _temporalio_worker_params() -> Set[str]:
    return {
        p.name
        for p in inspect.signature(
            temporalio.worker.Worker.__init__
        ).parameters.values()
        if p.name != "self"
        and p.kind
        not in (inspect.Parameter.VAR_KEYWORD, inspect.Parameter.VAR_POSITIONAL)
    }


def _our_worker_explicit_params() -> Set[str]:
    return {
        p.name
        for p in inspect.signature(Worker.__init__).parameters.values()
        if p.name not in ("self", "config")
        and p.kind
        not in (inspect.Parameter.VAR_KEYWORD, inspect.Parameter.VAR_POSITIONAL)
    }


def test_every_worker_param_is_classified() -> None:
    actual = _temporalio_worker_params()
    classified = HONORED | REJECTED | DEVIATION | set(INERT)

    unclassified = actual - classified
    assert not unclassified, (
        f"temporalio Worker params not classified in the audit: {sorted(unclassified)} "
        "— add each to HONORED / INERT / REJECTED / DEVIATION."
    )
    stale = classified - actual
    assert (
        not stale
    ), f"audit classifies names that aren't temporalio Worker params: {sorted(stale)}"


def test_buckets_are_disjoint() -> None:
    buckets = [HONORED, REJECTED, DEVIATION, set(INERT)]
    for i, a in enumerate(buckets):
        for b in buckets[i + 1 :]:
            assert not (a & b), f"param classified in two buckets: {sorted(a & b)}"


def test_inert_reasons_present() -> None:
    assert all(reason.strip() for reason in INERT.values())


def test_honored_params_are_explicitly_accepted() -> None:
    # Every honored param must be a named parameter on our Worker (not swallowed
    # by **unsupported) — that is what makes "honored" verifiable.
    ours = _our_worker_explicit_params()
    not_accepted = HONORED - ours
    assert (
        not not_accepted
    ), f"honored but not an explicit Worker param: {sorted(not_accepted)}"


@workflow.defn
class _AuditWorkflow:
    @workflow.run
    async def run(self) -> None: ...


@pytest.mark.parametrize("option", sorted(REJECTED))
def test_rejected_option_raises(option: str) -> None:
    # The reject check runs before any DBOS construction, so no database is
    # needed. A truthy non-default value triggers it.
    rejected_kwargs: Dict[str, Any] = {option: [object()]}
    with pytest.raises(NotImplementedError, match=option):
        Worker(
            cast(DBOSConfig, {"name": "audit"}),
            task_queue="audit-tq",
            workflows=[_AuditWorkflow],
            **rejected_kwargs,
        )
