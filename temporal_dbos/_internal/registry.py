"""Workflow-type and activity-type registries.

Decorators (``@workflow.defn``, ``@activity.defn``) attach definitions to the
decorated object; worker setup (``dispatcher.register_worker``) copies them
in here, keyed by type name, where the dispatcher resolves them by name at
execution time. Re-registering a name replaces the definition — that is how
"fix the bug and redeploy" works for in-process workflow-task retries.
"""

import collections.abc
import inspect
import logging
from dataclasses import dataclass, field
from typing import (
    Any,
    Callable,
    Dict,
    List,
    Optional,
    Sequence,
    Tuple,
    Type,
    cast,
    get_args,
    get_origin,
)

logger = logging.getLogger("temporal_dbos")

WORKFLOW_DEFN_ATTR = "__temporal_workflow_definition"
RUN_ATTR = "__temporal_workflow_run"
WORKFLOW_NAME_ATTR = "__temporal_workflow_name"
SIGNAL_ATTR = "__temporal_signal_definition"
SIGNAL_POLICY_ATTR = "__temporal_signal_unfinished_policy"
SIGNAL_DESC_ATTR = "__temporal_signal_description"
QUERY_ATTR = "__temporal_query_definition"
QUERY_DESC_ATTR = "__temporal_query_description"
INIT_ATTR = "__temporal_workflow_init"
ACTIVITY_DEFN_ATTR = "__temporal_activity_definition"

# Sentinel for "no handler marker present": the marker value is the handler
# name, which is ``None`` for a *dynamic* handler — so absence can't be probed
# with a plain ``getattr(..., None)`` default.
_UNSET = object()

# Marker name for the single dynamic handler in each category (signal/query/
# update): a ``None`` key in the relevant dict. Mirrors temporalio, where a
# dynamic handler's definition name is ``None``.


@dataclass(frozen=True)
class SignalDefinition:
    # ``None`` name marks the *dynamic* (catch-all) signal handler, dispatched
    # as ``fn(self, name, Sequence[RawValue])`` for any unmatched signal.
    name: Optional[str]
    fn: Callable[..., Any]
    # HandlerUnfinishedPolicy value (int to avoid importing workflow here);
    # 1 = WARN_AND_ABANDON (the temporalio default).
    unfinished_policy: int = 1
    arg_types: Optional[List[type]] = None
    description: Optional[str] = None


@dataclass(frozen=True)
class QueryDefinition:
    name: Optional[str]  # ``None`` marks the dynamic query handler.
    fn: Callable[..., Any]
    arg_types: Optional[List[type]] = None
    ret_type: Optional[type] = None
    description: Optional[str] = None


@dataclass(frozen=True)
class UpdateDefinition:
    name: Optional[str]  # ``None`` marks the dynamic update handler.
    fn: Callable[..., Any]
    validator: Optional[Callable[..., Any]] = None
    unfinished_policy: int = 1
    arg_types: Optional[List[type]] = None
    ret_type: Optional[type] = None
    description: Optional[str] = None


@dataclass(frozen=True)
class WorkflowDefinition:
    name: str
    cls: Type[Any]
    run_fn: Callable[..., Any]
    # ``None`` key = the dynamic (catch-all) handler for that category.
    signals: Dict[Optional[str], SignalDefinition] = field(default_factory=dict)
    queries: Dict[Optional[str], QueryDefinition] = field(default_factory=dict)
    updates: Dict[Optional[str], UpdateDefinition] = field(default_factory=dict)
    init_takes_args: bool = False
    failure_exception_types: Tuple[Type[BaseException], ...] = ()
    # run() signature hints (from conversion.type_hints_from_func): arg_types
    # rebuilds typed run arguments from payloads; ret_type lets a local client
    # infer the result type when none is passed.
    arg_types: Optional[List[type]] = None
    ret_type: Optional[type] = None
    # @workflow.defn(versioning_behavior=...): stored for parity. PINNED is what
    # DBOS enforces anyway (recovery/dequeue scoped to application_version);
    # AUTO_UPGRADE has no DBOS analog (DEVIATIONS D29).
    versioning_behavior: Optional[int] = None


@dataclass(frozen=True)
class ActivityDefinition:
    name: str
    fn: Callable[..., Any]
    is_async: bool
    # Signature hints (conversion.type_hints_from_func): arg_types rebuilds the
    # activity's typed arguments; ret_type rebuilds its result for the caller.
    arg_types: Optional[List[type]] = None
    ret_type: Optional[type] = None
    # A *dynamic* activity (catch-all): invoked as ``fn(Sequence[RawValue])``
    # for any activity type with no exact registration (§6.1.2). ``name`` keeps
    # the function name for debugging but is never used to route to it.
    dynamic: bool = False


_workflows: Dict[str, WorkflowDefinition] = {}
_activities: Dict[str, ActivityDefinition] = {}
# The single dynamic activity registered with this process, if any.
_dynamic_activity: Optional[ActivityDefinition] = None

# Temporal type name -> the registered per-type DBOS workflow (`wf:{type}`),
# populated by dispatcher.register_worker. Lives here (not in dispatcher) so
# the interpreter can resolve child-workflow dispatch functions without an
# import cycle.
_dbos_workflows: Dict[str, Callable[..., Any]] = {}


def register_dbos_workflow(name: str, fn: Callable[..., Any]) -> None:
    _dbos_workflows[name] = fn


def dbos_workflow_for(name: str) -> Callable[..., Any]:
    fn = _dbos_workflows.get(name)
    if fn is None:
        raise KeyError(
            f"Workflow type {name!r} is not registered with this worker. "
            f"Registered types: {sorted(_dbos_workflows)}"
        )
    return fn


# The process-global ``__temporal_activity`` dispatcher (the cross-queue
# activity path, §6.1.2), registered by ``dispatcher.register_worker``. Stored
# here (not in activity_workflow) so the interpreter can resolve it for the
# enqueue without an import cycle, mirroring ``_dbos_workflows`` above.
_activity_dispatcher: Optional[Callable[..., Any]] = None


def register_activity_dispatcher_fn(fn: Callable[..., Any]) -> None:
    global _activity_dispatcher
    _activity_dispatcher = fn


def activity_dispatcher_fn() -> Callable[..., Any]:
    if _activity_dispatcher is None:
        raise KeyError(
            "The __temporal_activity dispatcher is not registered with this "
            "worker (no Worker has been constructed in this process)."
        )
    return _activity_dispatcher


# Worker-level failure exception types (Worker(workflow_failure_exception_types=...)),
# merged across workers in this process; checked by the interpreter alongside
# each definition's own list.
worker_failure_exception_types: Tuple[Type[BaseException], ...] = ()


def add_worker_failure_exception_types(
    types: Sequence[Type[BaseException]],
) -> None:
    global worker_failure_exception_types
    merged = dict.fromkeys(worker_failure_exception_types + tuple(types))
    worker_failure_exception_types = tuple(merged)


# Worker-level activity interceptors (Worker(interceptors=...)). The activity
# attempt step (activities.py) folds these around each attempt (DESIGN §6.8).
# Set (not merged): one Worker per process owns the interceptor list.
worker_interceptors: Tuple[Any, ...] = ()


def set_worker_interceptors(interceptors: Sequence[Any]) -> None:
    global worker_interceptors
    worker_interceptors = tuple(interceptors)


# The task queue this process's Worker dequeues from (one Worker per process).
# A workflow always runs on the worker that dequeued it, so this is its task
# queue — surfaced as workflow.info()/activity.info().task_queue. None when no
# Worker registered a queue (the in-process Phase-0 dispatcher harness).
worker_task_queue: Optional[str] = None


def set_worker_task_queue(task_queue: Optional[str]) -> None:
    global worker_task_queue
    worker_task_queue = task_queue


# The Temporal namespace this process serves (one per process — it maps to the
# DBOS system schema, which is process-global). Surfaced as
# workflow.info()/activity.info().namespace. None when no Worker is registered.
worker_namespace: Optional[str] = None


def set_worker_namespace(namespace: Optional[str]) -> None:
    global worker_namespace
    worker_namespace = namespace


# This process's worker deployment NAME, backing
# workflow.Info.get_current_deployment_version()/get_current_build_id(). Set by
# the Worker (deployment_config.version.deployment_name, else the DBOS app name);
# None when no Worker is active (the in-process dispatcher harness) → the
# accessors return None. The build_id half is NOT stored here: it is read live
# from the DBOS application_version at access time (the version DBOS actually
# pins recovery/dequeue to, including a code-hash for auto-versioning), so the
# surfaced version always equals the enforced one (DEVIATIONS D29).
worker_deployment_name: Optional[str] = None


def set_worker_deployment_name(name: Optional[str]) -> None:
    global worker_deployment_name
    worker_deployment_name = name


def workflow_definition_of(cls: Type[Any]) -> WorkflowDefinition:
    defn = cls.__dict__.get(WORKFLOW_DEFN_ATTR)
    if defn is None:
        raise TypeError(f"{cls.__qualname__} is missing the @workflow.defn decorator")
    assert isinstance(defn, WorkflowDefinition)
    return defn


def activity_definition_of(fn: Callable[..., Any]) -> ActivityDefinition:
    defn = getattr(fn, ACTIVITY_DEFN_ATTR, None)
    if defn is None:
        raise TypeError(
            f"{getattr(fn, '__qualname__', fn)} is missing the @activity.defn decorator"
        )
    assert isinstance(defn, ActivityDefinition)
    return defn


def register_workflow(defn: WorkflowDefinition) -> None:
    if defn.name in _workflows and _workflows[defn.name].cls is not defn.cls:
        logger.info("Replacing registered workflow type %r", defn.name)
    _workflows[defn.name] = defn


def lookup_workflow(name: str) -> WorkflowDefinition:
    defn = _workflows.get(name)
    if defn is None:
        raise KeyError(
            f"Workflow type {name!r} is not registered with this worker. "
            f"Registered types: {sorted(_workflows)}"
        )
    return defn


def register_activity(defn: ActivityDefinition) -> None:
    if defn.dynamic:
        # A dynamic activity is a fallback only, reachable for any unmatched
        # activity type — never registered under a name (mirroring temporalio,
        # where its definition name is None).
        global _dynamic_activity
        _dynamic_activity = defn
    else:
        _activities[defn.name] = defn


def lookup_activity(name: str) -> ActivityDefinition:
    defn = _activities.get(name)
    if defn is None:
        raise KeyError(
            f"Activity type {name!r} is not registered with this worker. "
            f"Registered types: {sorted(_activities)}"
        )
    return defn


def dynamic_activity() -> Optional[ActivityDefinition]:
    """The process's dynamic (catch-all) activity, or ``None``."""
    return _dynamic_activity


def require_dynamic_activity() -> ActivityDefinition:
    if _dynamic_activity is None:
        raise KeyError("No dynamic activity is registered with this worker")
    return _dynamic_activity


def build_workflow_definition(
    cls: Type[Any],
    *,
    name: Optional[str],
    failure_exception_types: Sequence[Type[BaseException]],
    versioning_behavior: Optional[int] = None,
) -> WorkflowDefinition:
    """Scan a @workflow.defn-decorated class for handler markers and validate,
    mirroring temporalio's decoration-time checks.
    """
    from .conversion import type_hints_from_func

    workflow_name = name if name is not None else cls.__name__

    run_fn: Optional[Callable[..., Any]] = None
    signals: Dict[Optional[str], SignalDefinition] = {}
    queries: Dict[Optional[str], QueryDefinition] = {}
    updates: Dict[Optional[str], UpdateDefinition] = {}
    seen_run_names: List[str] = []

    for attr_name, member in inspect.getmembers(cls):
        if getattr(member, RUN_ATTR, False):
            seen_run_names.append(attr_name)
            run_fn = member
        # A ``None`` marker is a dynamic handler (catch-all); _UNSET means the
        # member isn't a handler at all.
        signal_marker = getattr(member, SIGNAL_ATTR, _UNSET)
        if signal_marker is not _UNSET:
            signal_name = cast(Optional[str], signal_marker)  # None = dynamic
            if signal_name in signals:
                raise ValueError(_duplicate_handler_msg("signal", signal_name))
            sig_args, _ = type_hints_from_func(member)
            if signal_name is None:
                _validate_dynamic_handler_sig("signal", sig_args)
            signals[signal_name] = SignalDefinition(
                name=signal_name,
                fn=member,
                unfinished_policy=int(getattr(member, SIGNAL_POLICY_ATTR, 1)),
                arg_types=sig_args,
                description=getattr(member, SIGNAL_DESC_ATTR, None),
            )
        query_marker = getattr(member, QUERY_ATTR, _UNSET)
        if query_marker is not _UNSET:
            query_name = cast(Optional[str], query_marker)  # None = dynamic
            if query_name in queries:
                raise ValueError(_duplicate_handler_msg("query", query_name))
            q_args, q_ret = type_hints_from_func(member)
            if query_name is None:
                _validate_dynamic_handler_sig("query", q_args)
            queries[query_name] = QueryDefinition(
                name=query_name,
                fn=member,
                arg_types=q_args,
                ret_type=q_ret,
                description=getattr(member, QUERY_DESC_ATTR, None),
            )
        # Updates are wrapper objects (to carry .validator), not functions.
        from ..workflow import _UpdateMethod  # circular-import-safe at call time

        if isinstance(member, _UpdateMethod):
            if member.name in updates:
                raise ValueError(_duplicate_handler_msg("update", member.name))
            u_args, u_ret = type_hints_from_func(member.fn)
            if member.name is None:
                _validate_dynamic_handler_sig("update", u_args)
            updates[member.name] = UpdateDefinition(
                name=member.name,
                fn=member.fn,
                validator=member.validator_fn,
                unfinished_policy=int(member.unfinished_policy),
                arg_types=u_args,
                ret_type=u_ret,
                description=member.description,
            )

    if run_fn is None:
        raise ValueError("Missing @workflow.run method")
    if len(seen_run_names) > 1:
        raise ValueError(
            f"Multiple methods found for @workflow.run: {sorted(seen_run_names)}"
        )
    if not inspect.iscoroutinefunction(run_fn):
        raise ValueError("@workflow.run method must be an async function")

    init_takes_args = getattr(cls.__init__, INIT_ATTR, False)

    # Stamp the run method with the type name so client code can reference
    # workflows the temporalio way: client.execute_workflow(MyWorkflow.run, ...)
    setattr(run_fn, WORKFLOW_NAME_ATTR, workflow_name)

    # @workflow.init means __init__ takes the run args, so when present its
    # signature is authoritative for the argument types.
    arg_source = cls.__init__ if init_takes_args else run_fn
    arg_types, _ = type_hints_from_func(arg_source)
    _, ret_type = type_hints_from_func(run_fn)

    return WorkflowDefinition(
        name=workflow_name,
        cls=cls,
        run_fn=run_fn,
        signals=signals,
        queries=queries,
        updates=updates,
        init_takes_args=init_takes_args,
        failure_exception_types=tuple(failure_exception_types),
        arg_types=arg_types,
        ret_type=ret_type,
        versioning_behavior=versioning_behavior,
    )


def _duplicate_handler_msg(kind: str, name: Optional[str]) -> str:
    if name is None:
        return f"Multiple dynamic {kind} handlers found"
    return f"Multiple {kind} methods found for {name!r}"


def _is_raw_value_sequence(annotation: Any) -> bool:
    """Whether ``annotation`` is ``Sequence[RawValue]`` — written either as
    ``typing.Sequence`` or ``collections.abc.Sequence``. temporalio accepts
    both spellings, and ``get_type_hints`` preserves whichever the user wrote
    (the two are not ``==``), so match on origin + args rather than identity."""
    from ..common import RawValue

    return get_origin(annotation) is collections.abc.Sequence and get_args(
        annotation
    ) == (RawValue,)


def _validate_dynamic_handler_sig(kind: str, arg_types: Optional[List[type]]) -> None:
    """A dynamic signal/query/update handler must be
    ``(self, name: str, args: Sequence[RawValue])`` (mirroring temporalio's
    new-style dynamic handler)."""
    if (
        not arg_types
        or len(arg_types) != 2
        or arg_types[0] is not str
        or not _is_raw_value_sequence(arg_types[1])
    ):
        raise RuntimeError(
            f"Dynamic {kind} handler must accept (self, name: str, "
            "args: Sequence[RawValue])"
        )


def validate_dynamic_activity_sig(arg_types: Optional[List[type]]) -> None:
    """A dynamic activity must accept a single ``Sequence[RawValue]``
    (mirroring temporalio)."""
    if not arg_types or len(arg_types) != 1 or not _is_raw_value_sequence(arg_types[0]):
        raise TypeError("Dynamic activity must accept a single Sequence[RawValue]")
