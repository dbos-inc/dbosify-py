"""Signature parity against the real temporalio SDK (a dev-dependency).

This is the API-drift alarm (DESIGN §9): every public name temporal_dbos
exposes is diffed against its temporalio counterpart.

Rules enforced per callable:
  * a parameter we accept must exist in temporalio (no invented API),
  * shared parameters must agree on kind (keyword-only-ness) and on whether
    they carry a default, and appear in the same relative order,
  * temporalio parameters we don't accept yet must be recorded in
    KNOWN_MISSING_PARAMS — exactly. Implementing one without removing it
    from the ledger fails the test, so the ledger can't go stale.

DELIBERATE_DEVIATIONS skips items whose shape intentionally differs — it is
the formal record of where temporal-dbos diverges on purpose.
"""

import enum
import inspect
from typing import Any, Callable, Dict, List, Set, Tuple

import temporalio.activity
import temporalio.client
import temporalio.common
import temporalio.converter
import temporalio.exceptions
import temporalio.testing
import temporalio.worker
import temporalio.workflow

import temporal_dbos.activity
import temporal_dbos.client
import temporal_dbos.common
import temporal_dbos.converter
import temporal_dbos.exceptions
import temporal_dbos.testing
import temporal_dbos.worker
import temporal_dbos.workflow

MODULE_PAIRS = {
    "workflow": (temporal_dbos.workflow, temporalio.workflow),
    "activity": (temporal_dbos.activity, temporalio.activity),
    "client": (temporal_dbos.client, temporalio.client),
    "worker": (temporal_dbos.worker, temporalio.worker),
    "common": (temporal_dbos.common, temporalio.common),
    "converter": (temporal_dbos.converter, temporalio.converter),
    "exceptions": (temporal_dbos.exceptions, temporalio.exceptions),
    "testing": (temporal_dbos.testing, temporalio.testing),
}

# Names/methods whose shape deliberately differs. qualname -> reason.
#
# Worker.__init__ / Client.__init__ / Client.connect are whole-callable
# exemptions: their leading parameter fundamentally diverges (a DBOSConfig /
# DBOSClient, not target_host), and a ``**unsupported`` catch-all deliberately
# absorbs temporalio's many gRPC-era parameters. The catch-all is also the
# blind spot that hid the missing ``interceptors=`` — a supported parameter
# could be silently swallowed instead of explicitly accepted — so the
# parameters we *do* honor are guarded positively by
# ``test_supported_params_explicitly_accepted`` (EXPLICITLY_ACCEPTED_PARAMS).
DELIBERATE_DEVIATIONS: Dict[str, str] = {
    "client.Client.__init__": "wraps a dbos.DBOSClient (DESIGN §5, revised)",
    "client.Client.connect": "takes dbos.DBOSClient instead of target_host",
    "worker.Worker.__init__": "takes dbos.DBOSConfig; one worker per process",
    "testing.WorkflowEnvironment.start_local": (
        "provisions a database on env-provided Postgres; temporalio's "
        "params are all dev-server flags, which don't apply"
    ),
    "testing.WorkflowEnvironment.start_time_skipping": (
        "Phase 4; raises NotImplementedError"
    ),
    "testing.WorkflowEnvironment.dbos_config": (
        "DBOS-native extension: the config to build the env's Worker from "
        "(Workers take a DBOSConfig, not a client)"
    ),
    "converter.Payload": (
        "our own lightweight Payload; temporalio's is the protobuf "
        "temporalio.api.common.v1.Payload — protobuf payloads are unsupported "
        "(corollary of D1, no non-Python clients)"
    ),
    "client.ScheduleAsyncIterator.__init__": (
        "wraps a pre-fetched page of DBOS schedule rows, not a gRPC paginator "
        "(DESIGN §6.7); the async-iteration contract is identical"
    ),
    "client.WorkflowExecutionAsyncIterator.__init__": (
        "pages via DBOS limit/offset (DESIGN §6.2) instead of a gRPC cursor + "
        "ListWorkflowsInput; the async-iteration contract is identical"
    ),
    "worker.Replayer.__init__": (
        "re-executes DBOS step checkpoints in this process's runtime (DEVIATIONS "
        "D27); server/sandbox params (namespace, build_id, identity, "
        "workflow_runner, debug_mode, runtime, plugins, ...) have no analog and "
        "are accepted-and-ignored — honored params guarded by "
        "EXPLICITLY_ACCEPTED_PARAMS"
    ),
    "client.WorkflowHistory.__init__": (
        "DB-bound: carries a run's DBOS step checkpoints (run_id, workflow_type, "
        "recorded_steps, attributes, app_version), not a Temporal event log "
        "(DEVIATIONS D27)"
    ),
    "client.WorkflowHistory.replay_horizon": (
        "DBOS-native helper: the recorded checkpoint horizon (max function_id)"
    ),
    "client.WorkflowHistory.step_count": (
        "DBOS-native helper: number of recorded step checkpoints"
    ),
    "client.WorkflowHandle.fetch_history_events": (
        "no Temporal event history; raises NotImplementedError pointing at "
        "fetch_history (DEVIATIONS D27)"
    ),
}

# temporalio parameters not accepted, recorded exactly. qualname -> parameter
# names. Implementing a parameter requires deleting it here.
KNOWN_MISSING_PARAMS: Dict[str, Set[str]] = {
    # no-analog timeouts (exec/task, unenforced); protobuf raw_memo; root not held
    "workflow.Info.__init__": {
        "execution_timeout",
        "raw_memo",
        "root",
        "task_timeout",
    },
    # no event-log history_length; protobuf raw_info; transitive root not held
    "client.WorkflowExecution.__init__": {
        "history_length",
        "raw_info",
        "root_id",
        "root_run_id",
    },
    # as WorkflowExecution, plus protobuf raw_description
    "client.WorkflowExecutionDescription.__init__": {
        "history_length",
        "raw_description",
        "raw_info",
        "root_id",
        "root_run_id",
    },
    # gRPC start-response object, no analog
    "client.WorkflowHandle.__init__": {
        "start_workflow_response",
    },
    # no archival tier; fetch_history reads DBOS step checkpoints
    "client.WorkflowHandle.fetch_history": {"skip_archival"},
    # gRPC-era callbacks/links/stack_level (versioning_override is accepted
    # and inert — DEVIATIONS D29)
    "client.Client.start_workflow": {
        "callbacks",
        "links",
        "stack_level",
    },
    # gRPC-era stack_level; versioning_override: no DBOS analog (D27/D28)
    "client.WithStartWorkflowOperation.__init__": {
        "stack_level",
        "versioning_override",
    },
    # ActivityCancellationDetails (the cancel reason) not implemented
    "testing.ActivityEnvironment.cancel": {"cancellation_details"},
    # no protobuf Failure to fill in place; we return the failure envelope instead
    "converter.FailureConverter.to_failure": {"failure"},
    # no protobuf Failure to fill in place; we return the failure envelope instead
    "converter.DefaultFailureConverter.to_failure": {"failure"},
    # no protobuf Failure to fill in place; we return the failure envelope instead
    "converter.DataConverter.encode_failure": {"failure"},
    # external storage + payload-size limits not implemented (DESIGN §6.9)
    "converter.DataConverter.__init__": {"external_storage", "payload_limits"},
    # interceptor headers on scheduled starts not propagated; protobuf raw_info
    "client.ScheduleActionStartWorkflow.__init__": {
        "headers",
        "raw_info",
    },
    # schedules aren't searchable entities (own SAs); no converter handle; protobuf raw
    "client.ScheduleDescription.__init__": {
        "data_converter",
        "raw_description",
        "search_attributes",
        "typed_search_attributes",
    },
    # as ScheduleDescription; protobuf raw_entry
    "client.ScheduleListDescription.__init__": {
        "data_converter",
        "raw_entry",
        "search_attributes",
        "typed_search_attributes",
    },
}


# Parameters that must be accepted *explicitly* (named in the signature, not
# merely swallowed by a ``**unsupported`` catch-all) on the whole-callable
# DELIBERATE_DEVIATIONS above. This is the positive guard the catch-all design
# needs: it is what would have caught the absent ``interceptors=``. qualname ->
# required keyword parameter names. Every name here must also exist on the
# temporalio counterpart (asserted by the test), so this can't drift into
# inventing API.
EXPLICITLY_ACCEPTED_PARAMS: Dict[str, Set[str]] = {
    # (Worker's ``data_converter`` is a deliberate temporal_dbos extension — it
    # has no temporalio Worker counterpart — so it isn't listed here; every
    # name below must exist on temporalio's Worker.)
    "worker.Worker.__init__": {
        "task_queue",
        "workflows",
        "activities",
        "workflow_failure_exception_types",
        "interceptors",
        "build_id",
        "use_worker_versioning",
        "deployment_config",
    },
    "client.Client.__init__": {"data_converter", "interceptors"},
    "client.Client.connect": {"data_converter", "interceptors"},
    "worker.Replayer.__init__": {
        "workflows",
        "data_converter",
        "interceptors",
        "workflow_failure_exception_types",
    },
}


def _resolve_callable(qualname: str) -> Any:
    module_key, _, attr_path = qualname.partition(".")
    obj: Any = MODULE_PAIRS[module_key][0]
    for part in attr_path.split("."):
        obj = getattr(obj, part)
    return obj


def _public_names(module: Any) -> List[str]:
    if hasattr(module, "__all__"):
        return list(module.__all__)
    names = []
    for name, obj in vars(module).items():
        if name.startswith("_") or inspect.ismodule(obj):
            continue
        if inspect.isclass(obj) or inspect.isfunction(obj):
            if getattr(obj, "__module__", None) == module.__name__:
                names.append(name)
        else:
            names.append(name)  # constants/instances: existence-check only
    return names


def _params_of(fn: Callable[..., Any]) -> List[inspect.Parameter]:
    return [p for p in inspect.signature(fn).parameters.values() if p.name != "self"]


def _compare_callable(
    qualname: str,
    ours: Callable[..., Any],
    theirs: Callable[..., Any],
    problems: List[str],
    *,
    check_defaults: bool = True,
) -> None:
    if qualname in DELIBERATE_DEVIATIONS:
        return
    try:
        our_params = _params_of(ours)
        their_params = _params_of(theirs)
    except (ValueError, TypeError):
        return  # builtins / non-introspectable
    variadic = (inspect.Parameter.VAR_KEYWORD, inspect.Parameter.VAR_POSITIONAL)
    theirs_by_name = {p.name: p for p in their_params}
    their_order = [p.name for p in their_params]

    shared_positions = []
    for p in our_params:
        if p.kind in variadic:
            continue  # our accept-and-ignore catch-alls
        tp = theirs_by_name.get(p.name)
        if tp is None:
            problems.append(
                f"{qualname}: parameter {p.name!r} does not exist in temporalio"
            )
            continue
        if p.kind != tp.kind:
            problems.append(
                f"{qualname}: parameter {p.name!r} kind {p.kind!s} != "
                f"temporalio's {tp.kind!s}"
            )
        if check_defaults and (p.default is inspect.Parameter.empty) != (
            tp.default is inspect.Parameter.empty
        ):
            problems.append(
                f"{qualname}: parameter {p.name!r} default-presence differs "
                f"from temporalio"
            )
        shared_positions.append(their_order.index(p.name))
    if shared_positions != sorted(shared_positions):
        problems.append(
            f"{qualname}: shared parameters are ordered differently than in "
            f"temporalio"
        )

    our_names = {p.name for p in our_params}
    missing = {
        p.name
        for p in their_params
        if p.name not in our_names
        and p.kind not in variadic
        and not p.name.startswith("_")  # private params aren't contract
    }
    recorded = KNOWN_MISSING_PARAMS.get(qualname, set())
    if missing != recorded:
        problems.append(
            f"{qualname}: missing-parameter ledger out of date: actually "
            f"missing {sorted(missing)}, recorded {sorted(recorded)}"
        )


def _compare_enum(
    qualname: str, ours: "type[enum.Enum]", theirs: type, problems: List[str]
) -> None:
    for member in ours:
        their_member = getattr(theirs, member.name, None)
        if their_member is None:
            problems.append(
                f"{qualname}.{member.name}: enum member does not exist in temporalio"
            )
        elif member.value != their_member.value:
            problems.append(
                f"{qualname}.{member.name}: value {member.value!r} != "
                f"temporalio's {their_member.value!r}"
            )


def _compare_class(
    qualname: str, ours: type, theirs: type, problems: List[str]
) -> None:
    if issubclass(ours, enum.Enum):
        _compare_enum(qualname, ours, theirs, problems)
        return
    dataclass_fields = getattr(ours, "__dataclass_fields__", {})
    missing_sentinel = object()
    for name in vars(ours):
        if name.startswith("_") and name != "__init__":
            continue
        if name in dataclass_fields:
            continue  # field defaults appear as class attrs; __init__ covers them
        if f"{qualname}.{name}" in DELIBERATE_DEVIATIONS:
            continue
        our_member = inspect.getattr_static(ours, name)
        their_member = inspect.getattr_static(theirs, name, missing_sentinel)
        if their_member is missing_sentinel:
            problems.append(
                f"{qualname}.{name}: does not exist on temporalio's {theirs.__name__}"
            )
            continue
        if isinstance(our_member, property):
            continue  # existence is enough for properties
        our_fn = _underlying_function(our_member)
        their_fn = _underlying_function(their_member)
        if our_fn is not None and their_fn is not None:
            # Dataclass-style Infos are SDK-constructed, never user-called,
            # so default-presence on their fields is not part of the contract.
            check_defaults = name != "__init__" or not hasattr(
                ours, "__dataclass_fields__"
            )
            _compare_callable(
                f"{qualname}.{name}",
                our_fn,
                their_fn,
                problems,
                check_defaults=check_defaults,
            )


def _underlying_function(member: Any) -> Any:
    if isinstance(member, (staticmethod, classmethod)):
        return member.__func__
    if inspect.isfunction(member):
        return member
    return None


def _compare_module(key: str) -> List[str]:
    ours_mod, theirs_mod = MODULE_PAIRS[key]
    problems: List[str] = []
    for name in _public_names(ours_mod):
        qualname = f"{key}.{name}"
        if qualname in DELIBERATE_DEVIATIONS:
            continue
        ours = getattr(ours_mod, name)
        theirs = getattr(theirs_mod, name, None)
        if theirs is None:
            problems.append(f"{qualname}: does not exist in temporalio.{key}")
            continue
        if inspect.isclass(ours) and inspect.isclass(theirs):
            _compare_class(qualname, ours, theirs, problems)
        elif callable(ours) and callable(theirs):
            _compare_callable(qualname, ours, theirs, problems)
    return problems


def _format(problems: List[str]) -> str:
    return "\n".join(f"  - {p}" for p in problems)


import pytest  # noqa: E402


@pytest.mark.parametrize("module_key", sorted(MODULE_PAIRS))
def test_all_is_complete_and_accurate(module_key: str) -> None:
    """Every public class/function defined in a facade module must be
    declared in its ``__all__`` (else it silently escapes parity checking),
    and every ``__all__`` entry must actually exist.

    Constants and instances (e.g. ``logger``) can't be attributed to a
    defining module, so they are exempt from the completeness half — declare
    them anyway.
    """
    ours_mod, _ = MODULE_PAIRS[module_key]
    assert hasattr(ours_mod, "__all__"), f"{ours_mod.__name__} must declare __all__"
    declared = set(ours_mod.__all__)

    stale = sorted(name for name in declared if not hasattr(ours_mod, name))
    assert not stale, f"{ours_mod.__name__}.__all__ lists nonexistent names: {stale}"

    undeclared = sorted(
        name
        for name, obj in vars(ours_mod).items()
        if not name.startswith("_")
        and (inspect.isclass(obj) or inspect.isfunction(obj))
        and getattr(obj, "__module__", None) == ours_mod.__name__
        and name not in declared
    )
    assert not undeclared, (
        f"{ours_mod.__name__} defines public names missing from __all__ "
        f"(add them — or prefix with _ if private): {undeclared}"
    )


@pytest.mark.parametrize("module_key", sorted(MODULE_PAIRS))
def test_signature_parity(module_key: str) -> None:
    problems = _compare_module(module_key)
    assert not problems, (
        f"signature drift vs temporalio in {module_key!r}:\n{_format(problems)}\n"
        "Fix the signature, or record the change in KNOWN_MISSING_PARAMS / "
        "DELIBERATE_DEVIATIONS with a reason."
    )


@pytest.mark.parametrize("qualname", sorted(EXPLICITLY_ACCEPTED_PARAMS))
def test_supported_params_explicitly_accepted(qualname: str) -> None:
    """Guards the ``**unsupported`` blind spot on whole-exempt callables: each
    parameter we honor must be a named parameter (not silently absorbed by the
    catch-all), and must exist on the temporalio counterpart (no invented API).
    Dropping ``interceptors=`` from Worker/Client now fails here."""
    module_key, _, attr_path = qualname.partition(".")
    ours = _resolve_callable(qualname)
    their_obj: Any = MODULE_PAIRS[module_key][1]
    for part in attr_path.split("."):
        their_obj = getattr(their_obj, part)

    accepted = {
        p.name
        for p in _params_of(ours)
        if p.kind
        not in (inspect.Parameter.VAR_KEYWORD, inspect.Parameter.VAR_POSITIONAL)
    }
    their_names = {p.name for p in _params_of(their_obj)}

    required = EXPLICITLY_ACCEPTED_PARAMS[qualname]
    not_accepted = sorted(required - accepted)
    assert not not_accepted, (
        f"{qualname}: supported parameters not accepted explicitly (only via "
        f"**unsupported, or missing): {not_accepted}"
    )
    not_in_temporalio = sorted(required - their_names)
    assert not not_in_temporalio, (
        f"{qualname}: EXPLICITLY_ACCEPTED_PARAMS lists names absent from "
        f"temporalio (invented API): {not_in_temporalio}"
    )
