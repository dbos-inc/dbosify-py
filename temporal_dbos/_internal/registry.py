"""Workflow-type and activity-type registries.

Decorators (``@workflow.defn``, ``@activity.defn``) attach definitions to the
decorated object; worker setup (``dispatcher.register_worker``) copies them
in here, keyed by type name, where the dispatcher resolves them by name at
execution time. Re-registering a name replaces the definition — that is how
"fix the bug and redeploy" works for in-process workflow-task retries.
"""

import inspect
import logging
from dataclasses import dataclass, field
from typing import Any, Callable, Dict, List, Optional, Sequence, Tuple, Type

logger = logging.getLogger("temporal_dbos")

WORKFLOW_DEFN_ATTR = "__temporal_workflow_definition"
RUN_ATTR = "__temporal_workflow_run"
WORKFLOW_NAME_ATTR = "__temporal_workflow_name"
SIGNAL_ATTR = "__temporal_signal_definition"
QUERY_ATTR = "__temporal_query_definition"
INIT_ATTR = "__temporal_workflow_init"
ACTIVITY_DEFN_ATTR = "__temporal_activity_definition"


@dataclass(frozen=True)
class SignalDefinition:
    name: str
    fn: Callable[..., Any]


@dataclass(frozen=True)
class QueryDefinition:
    name: str
    fn: Callable[..., Any]


@dataclass(frozen=True)
class UpdateDefinition:
    name: str
    fn: Callable[..., Any]
    validator: Optional[Callable[..., Any]] = None


@dataclass(frozen=True)
class WorkflowDefinition:
    name: str
    cls: Type[Any]
    run_fn: Callable[..., Any]
    signals: Dict[str, SignalDefinition] = field(default_factory=dict)
    queries: Dict[str, QueryDefinition] = field(default_factory=dict)
    updates: Dict[str, UpdateDefinition] = field(default_factory=dict)
    init_takes_args: bool = False
    failure_exception_types: Tuple[Type[BaseException], ...] = ()


@dataclass(frozen=True)
class ActivityDefinition:
    name: str
    fn: Callable[..., Any]
    is_async: bool


_workflows: Dict[str, WorkflowDefinition] = {}
_activities: Dict[str, ActivityDefinition] = {}

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
    _activities[defn.name] = defn


def lookup_activity(name: str) -> ActivityDefinition:
    defn = _activities.get(name)
    if defn is None:
        raise KeyError(
            f"Activity type {name!r} is not registered with this worker. "
            f"Registered types: {sorted(_activities)}"
        )
    return defn


def build_workflow_definition(
    cls: Type[Any],
    *,
    name: Optional[str],
    failure_exception_types: Sequence[Type[BaseException]],
) -> WorkflowDefinition:
    """Scan a @workflow.defn-decorated class for handler markers and validate,
    mirroring temporalio's decoration-time checks.
    """
    workflow_name = name if name is not None else cls.__name__

    run_fn: Optional[Callable[..., Any]] = None
    signals: Dict[str, SignalDefinition] = {}
    queries: Dict[str, QueryDefinition] = {}
    updates: Dict[str, UpdateDefinition] = {}
    seen_run_names: List[str] = []

    for attr_name, member in inspect.getmembers(cls):
        if getattr(member, RUN_ATTR, False):
            seen_run_names.append(attr_name)
            run_fn = member
        signal_name = getattr(member, SIGNAL_ATTR, None)
        if signal_name is not None:
            if signal_name in signals:
                raise ValueError(f"Multiple signal methods found for {signal_name!r}")
            signals[signal_name] = SignalDefinition(name=signal_name, fn=member)
        query_name = getattr(member, QUERY_ATTR, None)
        if query_name is not None:
            if query_name in queries:
                raise ValueError(f"Multiple query methods found for {query_name!r}")
            queries[query_name] = QueryDefinition(name=query_name, fn=member)
        # Updates are wrapper objects (to carry .validator), not functions.
        from ..workflow import _UpdateMethod  # circular-import-safe at call time

        if isinstance(member, _UpdateMethod):
            if member.name in updates:
                raise ValueError(f"Multiple update methods found for {member.name!r}")
            updates[member.name] = UpdateDefinition(
                name=member.name, fn=member.fn, validator=member.validator_fn
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

    return WorkflowDefinition(
        name=workflow_name,
        cls=cls,
        run_fn=run_fn,
        signals=signals,
        queries=queries,
        updates=updates,
        init_takes_args=init_takes_args,
        failure_exception_types=tuple(failure_exception_types),
    )
