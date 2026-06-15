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
}

# temporalio parameters not accepted yet, recorded exactly. qualname ->
# parameter names. Implementing a parameter requires deleting it here.
KNOWN_MISSING_PARAMS: Dict[str, Set[str]] = {
    # Dynamic handlers + handler descriptions: Phase 3.
    # versioning_behavior: Phase 4; no_thread_cancel_exception: Phase 3
    # (activity cancellation types).
    "workflow.defn": {"dynamic", "versioning_behavior"},
    "workflow.signal": {"description", "dynamic"},
    "workflow.query": {"description", "dynamic"},
    "workflow.update": {"description", "dynamic"},
    "activity.defn": {"dynamic", "no_thread_cancel_exception"},
    # Info/describe field coverage grows with features (DESIGN §6.8).
    "workflow.Info.__init__": {
        "execution_timeout",
        "first_execution_run_id",
        "headers",
        "parent",
        "priority",
        "raw_memo",
        "root",
        "search_attributes",
        "task_timeout",
        "typed_search_attributes",
        "workflow_start_time",
    },
    "activity.Info.__init__": {
        "activity_run_id",
        "current_attempt_scheduled_time",
        "heartbeat_timeout",
        "namespace",
        "priority",
        "retry_policy",
        "schedule_to_close_timeout",
        "scheduled_time",
        "start_to_close_timeout",
        "started_time",
        "workflow_namespace",
    },
    "client.WorkflowExecution.__init__": {
        "execution_time",
        "history_length",
        "namespace",
        "parent_run_id",
        "raw_info",
        "root_id",
        "root_run_id",
        "search_attributes",
        "typed_search_attributes",
    },
    "client.WorkflowExecutionDescription.__init__": {
        "execution_time",
        "history_length",
        "namespace",
        "parent_run_id",
        "raw_description",
        "raw_info",
        "root_id",
        "root_run_id",
        "search_attributes",
        "typed_search_attributes",
    },
    "client.WorkflowHandle.__init__": {
        "start_workflow_response",
    },
    # Callbacks/links/stack_level are gRPC-era plumbing; versioning
    # overrides are Phase 4.
    "client.Client.start_workflow": {
        "callbacks",
        "links",
        "stack_level",
        "versioning_override",
    },
    "client.Client.execute_workflow": {"versioning_override"},
    "client.WithStartWorkflowOperation.__init__": {
        "stack_level",
        "versioning_override",
    },
    # Custom data converters arrive with the Phase 3 conversion pipeline.
    "client.AsyncActivityHandle.__init__": {"data_converter_override"},
    # Activity cancellation details: Phase 3.
    "testing.ActivityEnvironment.cancel": {"cancellation_details"},
    # We have no protobuf Failure to mutate in place, so the failure converter
    # *returns* the failure envelope instead of filling a passed-in `failure`.
    "converter.FailureConverter.to_failure": {"failure"},
    "converter.DefaultFailureConverter.to_failure": {"failure"},
    "converter.DataConverter.encode_failure": {"failure"},
    # External storage and payload-size limits are not implemented (DESIGN §6.9
    # scope); proto/search-attribute helpers are intentionally absent.
    "converter.DataConverter.__init__": {"external_storage", "payload_limits"},
}


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
