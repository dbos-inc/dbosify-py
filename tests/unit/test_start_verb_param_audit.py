"""Accepted-parameter audit for the *start verbs* (DESIGN §9), the third audit
alongside the Worker and Client constructor audits. The start verbs
(``Client.start_workflow``, ``workflow.start_child_workflow``,
``workflow.start_activity``, ``workflow.continue_as_new``) accept every
temporalio parameter as a named argument and then act on some while
debug-logging the rest. This audit classifies each parameter — honored, inert
(no behavioral analog in our model), or pending (behavior-changing and not yet
enforced, accepted-and-logged rather than silently dropped without record) — and
machine-checks completeness: a new temporalio parameter on any of these verbs
fails the test until it is classified, so no behavior-changing option can be
silently swallowed unnoticed.

The pending set is the point of the audit: it is the explicit, reviewed list of
behavior-changing options we accept but do not yet honor (see DEVIATIONS D35).
``execute_*`` variants share the start verbs' parameter surface and impl path;
their signatures are pinned separately by ``test_signature_parity``.
"""

from typing import Any, Callable, Dict, Mapping, Set

import pytest
import temporalio.client
import temporalio.workflow

from temporal_dbos import workflow
from temporal_dbos.client import Client
from tests.unit._param_audit import (
    Bucket,
    assert_buckets_disjoint,
    assert_every_param_classified,
    assert_honored_are_named,
    assert_reasons_present,
    named_params,
)


class _Spec:
    def __init__(
        self,
        ours: Callable[..., Any],
        theirs: Callable[..., Any],
        honored: Set[str],
        inert: Dict[str, str],
        pending: Dict[str, str],
    ) -> None:
        self.ours = ours
        self.theirs = theirs
        self.honored = honored
        self.inert = inert
        self.pending = pending

    @property
    def buckets(self) -> Mapping[str, Bucket]:
        return {"honored": self.honored, "inert": self.inert, "pending": self.pending}


# --- Client.start_workflow ---------------------------------------------------
_START_WORKFLOW = _Spec(
    ours=Client.start_workflow,
    theirs=temporalio.client.Client.start_workflow,
    honored={
        "id",
        "task_queue",
        "result_type",
        "run_timeout",
        "id_reuse_policy",
        "id_conflict_policy",
        "retry_policy",
        "cron_schedule",
        "memo",
        "search_attributes",
        "start_delay",
        "start_signal",
        "start_signal_args",
    },
    inert={
        "task_timeout": "no workflow-task concept (no Temporal workflow tasks)",
        "static_summary": "static metadata surfaced only in Temporal UI; no UI",
        "static_details": "static metadata surfaced only in Temporal UI; no UI",
        "rpc_metadata": "gRPC call metadata; no Temporal gRPC (D1)",
        "rpc_timeout": "bounds a single gRPC RPC; no Temporal gRPC (D1)",
        "request_eager_start": "server-side eager-start optimization; no server (D1)",
        "priority": "task-queue priority; DBOS queues have no priority lanes",
        "request_id": "gRPC start-dedup id; DBOS dedups on workflow id (D2)",
        "versioning_override": "per-start version override; pinned default matches, "
        "auto-upgrade has no DBOS analog (D29)",
        "callbacks": "server-side completion callbacks; no server (D1)",
        "links": "event-level links for Temporal visibility; no server (D1)",
        "stack_level": "controls temporalio's own warning stacklevel; cosmetic",
    },
    pending={
        "execution_timeout": "whole-execution (run-chain) deadline not enforced; "
        "tied to the pending TIMED_OUT status work (D19/D35)",
    },
)

# --- workflow.start_child_workflow -------------------------------------------
_START_CHILD = _Spec(
    ours=workflow.start_child_workflow,
    theirs=temporalio.workflow.start_child_workflow,
    honored={
        "id",
        "task_queue",
        "result_type",
        "cancellation_type",
        "parent_close_policy",
        "run_timeout",
        "retry_policy",
        "memo",
        "search_attributes",
    },
    inert={
        "task_timeout": "no workflow-task concept (no Temporal workflow tasks)",
        "versioning_intent": "child version pinning has no DBOS analog (D29)",
        "static_summary": "static metadata surfaced only in Temporal UI; no UI",
        "static_details": "static metadata surfaced only in Temporal UI; no UI",
        "priority": "task-queue priority; DBOS queues have no priority lanes",
    },
    pending={
        "execution_timeout": "whole-execution deadline not enforced (D19/D35)",
        "cron_schedule": "cron child spawns a detached chain the parent-close "
        "sweep would need to follow; honored for top-level only (D35)",
        "id_reuse_policy": "children enforce reject-on-duplicate; other policies "
        "need run-chain resolution the child-start path lacks (D35)",
    },
)

# --- workflow.start_activity -------------------------------------------------
_START_ACTIVITY = _Spec(
    ours=workflow.start_activity,
    theirs=temporalio.workflow.start_activity,
    honored={
        "task_queue",
        "result_type",
        "schedule_to_close_timeout",
        "schedule_to_start_timeout",
        "start_to_close_timeout",
        "heartbeat_timeout",
        "retry_policy",
        "cancellation_type",
        "activity_id",
    },
    inert={
        "versioning_intent": "activity version pinning has no DBOS analog (D29)",
        "summary": "static metadata surfaced only in Temporal UI; no UI",
        "priority": "task-queue priority; DBOS queues have no priority lanes",
    },
    pending={},
)

# --- workflow.continue_as_new ------------------------------------------------
_CONTINUE_AS_NEW = _Spec(
    ours=workflow.continue_as_new,
    theirs=temporalio.workflow.continue_as_new,
    honored={
        "task_queue",
        "run_timeout",
        "retry_policy",
        "memo",
        "search_attributes",
    },
    inert={
        "task_timeout": "no workflow-task concept (no Temporal workflow tasks)",
        "versioning_intent": "version pinning has no DBOS analog (D29)",
        "initial_versioning_behavior": "ramping/auto-upgrade has no DBOS analog (D29)",
    },
    pending={},
)

SPECS: Dict[str, _Spec] = {
    "client.start_workflow": _START_WORKFLOW,
    "workflow.start_child_workflow": _START_CHILD,
    "workflow.start_activity": _START_ACTIVITY,
    "workflow.continue_as_new": _CONTINUE_AS_NEW,
}


@pytest.mark.parametrize("name", sorted(SPECS))
def test_every_verb_param_is_classified(name: str) -> None:
    spec = SPECS[name]
    assert_every_param_classified(name, named_params(spec.theirs), spec.buckets)


@pytest.mark.parametrize("name", sorted(SPECS))
def test_buckets_are_disjoint(name: str) -> None:
    assert_buckets_disjoint(name, SPECS[name].buckets)


@pytest.mark.parametrize("name", sorted(SPECS))
def test_honored_params_are_named_arguments(name: str) -> None:
    # "Honored" is only verifiable if we accept the option as a named argument
    # (not swallowed by **unsupported) and therefore can act on it.
    spec = SPECS[name]
    assert_honored_are_named(name, spec.honored, named_params(spec.ours))


@pytest.mark.parametrize("name", sorted(SPECS))
def test_inert_and_pending_reasons_present(name: str) -> None:
    assert_reasons_present(name, SPECS[name].buckets)
