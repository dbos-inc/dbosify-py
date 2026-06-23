"""Accepted-parameter audit for ``Worker``: every parameter of
``temporalio.worker.Worker.__init__`` is classified — honored, inert (accepted
but a defensible no-op), rejected (behavior-changing + unsupported → raises), or
a fundamental deviation. The completeness check is machine-enforced: a new
temporalio Worker parameter fails this test until it is classified, so no
behavior-changing option can be silently swallowed.
"""

from typing import Any, Dict, Mapping, Set, cast

import pytest
import temporalio.worker
from dbos import DBOSConfig

from dbosify import workflow
from dbosify.worker import _REJECTED_OPTIONS, Worker
from tests.unit._param_audit import (
    Bucket,
    assert_buckets_disjoint,
    assert_every_param_classified,
    assert_honored_are_named,
    assert_reasons_present,
    named_params,
)

NAME = "worker.Worker.__init__"

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
    "nexus_task_executor": "Nexus not supported",
    "workflow_runner": "no workflow sandbox (no-sandbox)",
    "unsandboxed_workflow_runner": "no workflow sandbox (no-sandbox)",
    "max_cached_workflows": "no sticky cache; workflows replay from DBOS checkpoints",
    "max_concurrent_nexus_tasks": "Nexus not supported",
    "max_concurrent_workflow_task_polls": "DBOS queue listener, not Temporal long-polling",
    "nonsticky_to_sticky_poll_ratio": "no sticky cache",
    "max_concurrent_activity_task_polls": "DBOS queue listener, not Temporal long-polling",
    "no_remote_activities": "cross-queue dispatch model, not remote-activity polling",
    "sticky_queue_schedule_to_start_timeout": "no sticky cache",
    "max_heartbeat_throttle_interval": "in-memory heartbeat model (failover)",
    "default_heartbeat_throttle_interval": "in-memory heartbeat model (failover)",
    "shared_state_manager": "no multiprocess activities (no-multiprocess-activities)",
    "debug_mode": "no Temporal-style workflow-task deadlock detector",
    "disable_eager_activity_execution": "no server-side eager-activity optimization",
    "disable_safe_workflow_eviction": "no sticky-cache eviction",
    "workflow_task_poller_behavior": "DBOS queue listener, not Temporal pollers",
    "activity_task_poller_behavior": "DBOS queue listener, not Temporal pollers",
    "nexus_task_poller_behavior": "Nexus not supported",
    "disable_payload_error_limit": "Temporal payload-size caps not enforced (blocking-stalls-worker)",
    "max_workflow_task_external_storage_concurrency": "no external payload storage",
}


# honored is a set; inert is {param: reason}; rejected/deviation are sets.
BUCKETS: Mapping[str, Bucket] = {
    "honored": HONORED,
    "inert": INERT,
    "rejected": REJECTED,
    "deviation": DEVIATION,
}


def _temporalio_worker_params() -> Set[str]:
    return named_params(temporalio.worker.Worker.__init__)


def _our_worker_explicit_params() -> Set[str]:
    # ``config`` is our leading DBOSConfig positional, not a temporalio option.
    return named_params(Worker.__init__, skip={"config"})


def test_every_worker_param_is_classified() -> None:
    assert_every_param_classified(NAME, _temporalio_worker_params(), BUCKETS)


def test_buckets_are_disjoint() -> None:
    assert_buckets_disjoint(NAME, BUCKETS)


def test_inert_reasons_present() -> None:
    assert_reasons_present(NAME, BUCKETS)


def test_honored_params_are_explicitly_accepted() -> None:
    # Every honored param must be a named parameter on our Worker (not swallowed
    # by **unsupported) — that is what makes "honored" verifiable.
    assert_honored_are_named(NAME, HONORED, _our_worker_explicit_params())


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
