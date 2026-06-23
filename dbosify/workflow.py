"""Workflow author API, mirroring ``temporalio.workflow``.

The definition decorators (``defn``/``run``/``signal``/``query``/``update``/
``init``) and the runtime functions the interpreter backs
(``execute_activity``, ``start_activity``, ``sleep``, ``wait_condition``,
deterministic time/randomness, ``info``). Signatures mirror temporalio;
parameters not yet honored are accepted and ignored with a debug log, never
an error.

Workflow code runs on the deterministic virtual event loop hosted by
``_internal/interpreter.py``; every function here resolves the interpreter
from the running loop and delegates.
"""

import asyncio
import contextvars
import inspect
import logging
import uuid as uuid_mod
from contextlib import AbstractContextManager, nullcontext
from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone
from enum import Enum, IntEnum
from random import Random
from typing import (
    TYPE_CHECKING,
    Any,
    Awaitable,
    Callable,
    Dict,
    Iterable,
    Iterator,
    List,
    Mapping,
    MutableMapping,
    NoReturn,
    Optional,
    Sequence,
    Tuple,
    Type,
    TypeVar,
    Union,
    overload,
)

from . import exceptions
from ._internal import registry as _registry
from ._internal.namespaces import DEFAULT_NAMESPACE

if TYPE_CHECKING:
    from ._internal.workflow_interceptor import WorkflowOutboundInterceptor
    from .converter import Payload
from .common import (
    Priority,
    RetryPolicy,
    SearchAttributes,
    SearchAttributeUpdate,
    TypedSearchAttributes,
    VersioningBehavior,
    WorkerDeploymentVersion,
    _warn_on_deprecated_search_attributes,
)
from .converter import PayloadConverter

__all__ = [
    "ActivityCancellationType",
    "ActivityHandle",
    "ChildWorkflowCancellationType",
    "ChildWorkflowHandle",
    "ContinueAsNewVersioningBehavior",
    "ExternalWorkflowHandle",
    "Info",
    "ParentClosePolicy",
    "all_handlers_finished",
    "as_completed",
    "cancellation_reason",
    "continue_as_new",
    "get_signal_handler",
    "set_signal_handler",
    "get_dynamic_signal_handler",
    "set_dynamic_signal_handler",
    "get_query_handler",
    "set_query_handler",
    "get_dynamic_query_handler",
    "set_dynamic_query_handler",
    "get_update_handler",
    "set_update_handler",
    "get_dynamic_update_handler",
    "set_dynamic_update_handler",
    "ContinueAsNewError",
    "current_update_info",
    "defn",
    "deprecate_patch",
    "get_current_details",
    "instance",
    "new_random",
    "NondeterminismError",
    "execute_activity",
    "execute_activity_class",
    "execute_activity_method",
    "execute_child_workflow",
    "execute_local_activity",
    "execute_local_activity_class",
    "execute_local_activity_method",
    "get_external_workflow_handle",
    "get_external_workflow_handle_for",
    "get_last_completion_result",
    "get_last_failure",
    "HandlerUnfinishedPolicy",
    "has_last_completion_result",
    "in_workflow",
    "info",
    "init",
    "ParentInfo",
    "logger",
    "LoggerAdapter",
    "memo",
    "memo_value",
    "now",
    "patched",
    "payload_converter",
    "query",
    "random",
    "random_seed",
    "ReadOnlyContextError",
    "register_random_seed_callback",
    "RootInfo",
    "run",
    "set_current_details",
    "signal",
    "sleep",
    "start_activity",
    "start_activity_class",
    "start_activity_method",
    "start_child_workflow",
    "start_local_activity",
    "start_local_activity_class",
    "start_local_activity_method",
    "time",
    "time_ns",
    "unsafe",
    "upsert_memo",
    "upsert_search_attributes",
    "UnfinishedSignalHandlersWarning",
    "UnfinishedUpdateHandlersWarning",
    "update",
    "UpdateInfo",
    "uuid4",
    "wait",
    "wait_condition",
]


def _maybe_runtime() -> Optional["_Runtime"]:
    try:
        loop: Optional[asyncio.AbstractEventLoop] = asyncio.get_running_loop()
    except RuntimeError:
        loop = None
    runtime = getattr(loop, "dbosify_runtime", None)
    return runtime if isinstance(runtime, _Runtime) else None


class LoggerAdapter(logging.LoggerAdapter):  # type: ignore[type-arg]
    """Adapter that adds workflow details to each message and suppresses
    output during replay (mirroring ``temporalio.workflow.LoggerAdapter``;
    without suppression, recovery would re-emit every log line the first
    execution already produced).

    Attributes:
        workflow_info_on_message: Append workflow info to each message.
            Default True.
        log_during_replay: Emit logs while replaying. Default False.
    """

    def __init__(
        self, logger: logging.Logger, extra: Optional[Mapping[str, Any]]
    ) -> None:
        super().__init__(logger, extra or {})
        self.workflow_info_on_message = True
        self.log_during_replay = False

    def process(
        self, msg: Any, kwargs: "MutableMapping[str, Any]"
    ) -> "tuple[Any, MutableMapping[str, Any]]":
        if self.workflow_info_on_message:
            runtime = _maybe_runtime()
            if runtime is not None:
                workflow_info = runtime.runtime_info()
                msg_extra = {
                    "attempt": workflow_info.attempt,
                    "namespace": workflow_info.namespace,
                    "run_id": workflow_info.run_id,
                    "task_queue": workflow_info.task_queue,
                    "workflow_id": workflow_info.workflow_id,
                    "workflow_type": workflow_info.workflow_type,
                }
                msg = f"{msg} ({msg_extra})"
        return msg, kwargs

    def isEnabledFor(self, level: int) -> bool:
        if not self.log_during_replay:
            runtime = _maybe_runtime()
            if runtime is not None and runtime.runtime_is_replaying():
                return False
        return super().isEnabledFor(level)

    @property
    def base_logger(self) -> logging.Logger:
        """Underlying logger usable for actions such as adding handlers."""
        assert isinstance(self.logger, logging.Logger)
        return self.logger


logger = LoggerAdapter(logging.getLogger("dbosify.workflow"), None)

_F = TypeVar("_F", bound=Callable[..., Any])
_CT = TypeVar("_CT", bound=type)

_arg_unset = object()

# The update currently being handled, surfaced by current_update_info(). Set by
# the interpreter around an update validator/handler.
_current_update_info: "contextvars.ContextVar[UpdateInfo]" = contextvars.ContextVar(
    "__dbosify_current_update_info"
)


class HandlerUnfinishedPolicy(Enum):
    """What to do when a workflow finishes while a signal/update handler is
    still running, mirroring ``temporalio.workflow.HandlerUnfinishedPolicy``.
    Either way the handler is abandoned (cancelled with the execution); the
    policy controls whether that emits a warning.

    Plain ``Enum`` (not ``IntEnum``) to match temporalio exactly: members do
    not compare equal to their integer value. Internally we store the int
    ``.value`` (see _internal/registry.py).
    """

    WARN_AND_ABANDON = 1
    ABANDON = 2


class UnfinishedUpdateHandlersWarning(RuntimeWarning):
    """The workflow exited before all update handlers completed."""


class UnfinishedSignalHandlersWarning(RuntimeWarning):
    """The workflow exited before all signal handlers completed."""


class ContinueAsNewVersioningBehavior(IntEnum):
    """Versioning behavior for the run created by :py:func:`continue_as_new`,
    mirroring ``temporalio.workflow.ContinueAsNewVersioningBehavior``.

    A continue-as-new run is a fresh DBOS workflow enqueued by the current
    worker, so it takes that worker's build ID (pinned). ``AUTO_UPGRADE`` /
    ``USE_RAMPING_VERSION`` have no DBOS analog (ARCHITECTURE worker-versioning).
    """

    UNSPECIFIED = 0
    AUTO_UPGRADE = 1
    USE_RAMPING_VERSION = 2


# ---------------------------------------------------------------------------
# Definition decorators
# ---------------------------------------------------------------------------


@overload
def defn(cls: _CT) -> _CT: ...


@overload
def defn(
    *,
    name: Optional[str] = None,
    sandboxed: bool = True,
    dynamic: bool = False,
    failure_exception_types: Sequence[Type[BaseException]] = [],
    versioning_behavior: VersioningBehavior = VersioningBehavior.UNSPECIFIED,
) -> Callable[[_CT], _CT]: ...


def defn(
    cls: Optional[_CT] = None,
    *,
    name: Optional[str] = None,
    sandboxed: bool = True,
    dynamic: bool = False,
    failure_exception_types: Sequence[Type[BaseException]] = [],
    versioning_behavior: VersioningBehavior = VersioningBehavior.UNSPECIFIED,
) -> Union[_CT, Callable[[_CT], _CT]]:
    """Decorator for workflow classes. ``sandboxed`` is accepted and ignored
    (dbosify runs no sandbox).

    ``versioning_behavior`` is accepted and stored. ``PINNED`` is what
    dbosify enforces anyway (DBOS pins recovery/dequeue to the build ID =
    ``application_version``); ``AUTO_UPGRADE`` has no DBOS analog and degrades to
    pinned (ARCHITECTURE worker-versioning).

    ``dynamic`` is **not supported**: a catch-all workflow has no
    ``wf:{type}`` registration to dispatch to, which conflicts with the
    one-DBOS-workflow-per-type model (ARCHITECTURE dynamic-handlers). Passing
    ``dynamic=True`` raises ``NotImplementedError``. (Dynamic *signal/query/
    update* handlers and dynamic *activities* are supported.)
    """
    if dynamic:
        raise NotImplementedError(
            "dbosify does not support dynamic workflows "
            "(@workflow.defn(dynamic=True)); register each workflow type explicitly."
        )

    def decorator(cls: _CT) -> _CT:
        defn = _registry.build_workflow_definition(
            cls,
            name=name,
            failure_exception_types=failure_exception_types,
            versioning_behavior=int(versioning_behavior),
        )
        setattr(cls, _registry.WORKFLOW_DEFN_ATTR, defn)
        return cls

    if cls is not None:
        return decorator(cls)
    return decorator


def run(fn: _F) -> _F:
    """Decorator for the workflow run method. Must be an async function."""
    if not inspect.iscoroutinefunction(fn):
        raise ValueError("Workflow run method must be an async function")
    setattr(fn, _registry.RUN_ATTR, True)
    return fn


@overload
def signal(fn: _F) -> _F: ...


@overload
def signal(
    *,
    name: Optional[str] = None,
    dynamic: bool = False,
    unfinished_policy: HandlerUnfinishedPolicy = HandlerUnfinishedPolicy.WARN_AND_ABANDON,
    description: Optional[str] = None,
) -> Callable[[_F], _F]: ...


def signal(
    fn: Optional[_F] = None,
    *,
    name: Optional[str] = None,
    dynamic: bool = False,
    unfinished_policy: HandlerUnfinishedPolicy = HandlerUnfinishedPolicy.WARN_AND_ABANDON,
    description: Optional[str] = None,
) -> Union[_F, Callable[[_F], _F]]:
    """Decorator for a workflow signal handler method.

    ``dynamic=True`` makes this the catch-all handler for any signal with no
    exact match; it must be ``(self, name: str, args: Sequence[RawValue])``
    and cannot also set ``name``. ``description`` is metadata.
    """
    if name is not None and dynamic:
        raise RuntimeError("Cannot provide name and dynamic boolean")

    def decorator(fn: _F) -> _F:
        marker = None if dynamic else (name if name is not None else fn.__name__)
        setattr(fn, _registry.SIGNAL_ATTR, marker)
        setattr(fn, _registry.SIGNAL_POLICY_ATTR, int(unfinished_policy.value))
        setattr(fn, _registry.SIGNAL_DESC_ATTR, description)
        return fn

    if fn is not None:
        return decorator(fn)
    return decorator


@overload
def query(fn: _F) -> _F: ...


@overload
def query(
    *,
    name: Optional[str] = None,
    dynamic: bool = False,
    description: Optional[str] = None,
) -> Callable[[_F], _F]: ...


def query(
    fn: Optional[_F] = None,
    *,
    name: Optional[str] = None,
    dynamic: bool = False,
    description: Optional[str] = None,
) -> Union[_F, Callable[[_F], _F]]:
    """Decorator for a workflow query handler method. Must be synchronous.

    ``dynamic=True`` makes this the catch-all handler for any query with no
    exact match; it must be ``(self, name: str, args: Sequence[RawValue])``
    and cannot also set ``name``. ``description`` is metadata.
    """
    if name is not None and dynamic:
        raise RuntimeError("Cannot provide name and dynamic boolean")

    def decorator(fn: _F) -> _F:
        if inspect.iscoroutinefunction(fn):
            raise ValueError("Query handlers must be synchronous in dbosify v0")
        marker = None if dynamic else (name if name is not None else fn.__name__)
        setattr(fn, _registry.QUERY_ATTR, marker)
        setattr(fn, _registry.QUERY_DESC_ATTR, description)
        return fn

    if fn is not None:
        return decorator(fn)
    return decorator


class _UpdateMethod:
    """Stand-in for temporalio's update method wrapper: carries the handler,
    its name, and an optional validator registered via ``@handler.validator``.
    """

    def __init__(
        self,
        fn: Callable[..., Any],
        name: Optional[str],
        unfinished_policy: "HandlerUnfinishedPolicy" = HandlerUnfinishedPolicy.WARN_AND_ABANDON,
        *,
        dynamic: bool = False,
        description: Optional[str] = None,
    ) -> None:
        self.fn = fn
        # ``None`` name marks the dynamic (catch-all) update handler.
        self.name = None if dynamic else (name if name is not None else fn.__name__)
        self.unfinished_policy = unfinished_policy
        self.description = description
        self.validator_fn: Optional[Callable[..., Any]] = None

    def __call__(self, *args: Any, **kwargs: Any) -> Any:
        return self.fn(*args, **kwargs)

    def validator(self, vfn: Callable[..., Any]) -> Callable[..., Any]:
        """Decorator for the update's validator. Must be synchronous."""
        if inspect.iscoroutinefunction(vfn):
            raise ValueError("Update validators must be synchronous")
        self.validator_fn = vfn
        return vfn


class ExternalWorkflowHandle:
    """Handle for interacting with an *external* workflow from within a
    workflow: signal and (cooperatively) cancel, both as checkpointed sends
    targeting the id's current run.
    """

    def __init__(
        self, runtime: "_Runtime", workflow_id: str, run_id: Optional[str] = None
    ) -> None:
        self._runtime = runtime
        self._id = workflow_id
        self._run_id = run_id

    @property
    def id(self) -> str:
        """ID of the external workflow."""
        return self._id

    @property
    def run_id(self) -> Optional[str]:
        """Run ID of the external workflow, if bound."""
        return self._run_id

    async def signal(
        self, signal: Any, arg: Any = _arg_unset, *, args: Sequence[Any] = []
    ) -> None:
        """Send a signal to the external workflow."""
        from ._internal.workflow_interceptor import SignalExternalWorkflowInput

        name = (
            signal
            if isinstance(signal, str)
            else getattr(signal, _registry.SIGNAL_ATTR)
        )
        input = SignalExternalWorkflowInput(
            signal=str(name),
            args=_resolve_args(arg, args),
            # Single namespace per process: the external workflow is in this
            # process's namespace (cross-namespace signaling isn't supported).
            namespace=_registry.worker_namespace or DEFAULT_NAMESPACE,
            workflow_id=self._id,
            workflow_run_id=self._run_id,
            headers={},
        )
        await self._runtime.runtime_outbound().signal_external_workflow(input)

    async def cancel(self, *, reason: str = "") -> None:
        """Request cooperative cancellation of the external workflow."""
        from ._internal import inbox as _inbox

        await self._runtime.runtime_send_to_workflow(
            self._run_id or self._id,
            _inbox.cancel_envelope(reason),
            resolve_chain=self._run_id is None,
        )


@overload
def update(fn: Callable[..., Any]) -> _UpdateMethod: ...


@overload
def update(
    *,
    name: Optional[str] = None,
    dynamic: bool = False,
    unfinished_policy: HandlerUnfinishedPolicy = HandlerUnfinishedPolicy.WARN_AND_ABANDON,
    description: Optional[str] = None,
) -> Callable[[Callable[..., Any]], _UpdateMethod]: ...


def update(
    fn: Optional[Callable[..., Any]] = None,
    *,
    name: Optional[str] = None,
    dynamic: bool = False,
    unfinished_policy: HandlerUnfinishedPolicy = HandlerUnfinishedPolicy.WARN_AND_ABANDON,
    description: Optional[str] = None,
) -> Union[_UpdateMethod, Callable[[Callable[..., Any]], _UpdateMethod]]:
    """Decorator for a workflow update handler method. Attach a validator
    with ``@my_update.validator``.

    ``dynamic=True`` makes this the catch-all handler for any update with no
    exact match; it must be ``(self, name: str, args: Sequence[RawValue])``
    and cannot also set ``name``. ``description`` is metadata.
    """
    if name is not None and dynamic:
        raise RuntimeError("Cannot provide name and dynamic boolean")

    def decorator(fn: Callable[..., Any]) -> _UpdateMethod:
        return _UpdateMethod(
            fn, name, unfinished_policy, dynamic=dynamic, description=description
        )

    if fn is not None:
        return decorator(fn)
    return decorator


def init(init_fn: _F) -> _F:
    """Decorator for ``__init__`` to receive the same arguments as run."""
    if init_fn.__name__ != "__init__":
        raise ValueError("@workflow.init may only be used on __init__")
    setattr(init_fn, _registry.INIT_ATTR, True)
    return init_fn


# ---------------------------------------------------------------------------
# Runtime context
# ---------------------------------------------------------------------------


class ActivityCancellationType(IntEnum):
    """How a workflow cancels an activity, mirroring
    ``temporalio.workflow.ActivityCancellationType``. All three are honored:
    cancellation is delivered into the running attempt (observed at its next
    ``activity.heartbeat()``), and WAIT_CANCELLATION_COMPLETED resolves an
    explicit ``handle.cancel()`` only on the activity's confirmation.
    (During a workflow-cancellation unwind the awaiting coroutine is already
    cancelled, so WAIT behaves like TRY_CANCEL there — the request is still
    delivered.)
    """

    TRY_CANCEL = 0
    WAIT_CANCELLATION_COMPLETED = 1
    ABANDON = 2


class ParentClosePolicy(IntEnum):
    """What to do with running children when the parent closes, mirroring
    ``temporalio.workflow.ParentClosePolicy``.
    """

    UNSPECIFIED = 0
    TERMINATE = 1
    ABANDON = 2
    REQUEST_CANCEL = 3


class ChildWorkflowCancellationType(IntEnum):
    """How a workflow cancels a child workflow, mirroring
    ``temporalio.workflow.ChildWorkflowCancellationType``. Cancellation is
    delivered as the child's cooperative-cancel envelope; the WAIT variants
    are approximated as TRY_CANCEL while the awaiter is gone (same
    constraint as activities).
    """

    ABANDON = 0
    TRY_CANCEL = 1
    WAIT_CANCELLATION_COMPLETED = 2
    WAIT_CANCELLATION_REQUESTED = 3


@dataclass(frozen=True)
class ParentInfo:
    """Information about the parent workflow, mirroring
    ``temporalio.workflow.ParentInfo``. Present on :py:attr:`Info.parent`
    only when this run was started as a child of a workflow on a *different*
    run chain (a same-chain link is a continuation, not a parent)."""

    namespace: str
    run_id: str
    workflow_id: str


@dataclass(frozen=True)
class RootInfo:
    """Information about the root workflow of this run's tree, mirroring
    ``temporalio.workflow.RootInfo``. Present on :py:attr:`Info.root` only for
    descendants (a child/grandchild started cross-chain); ``None`` for a
    top-level workflow, which is itself the root."""

    run_id: str
    workflow_id: str


@dataclass(frozen=True)
class UpdateInfo:
    """Information about a workflow update in progress, mirroring
    ``temporalio.workflow.UpdateInfo``. Retrieved via
    :py:func:`current_update_info` inside an update handler/validator."""

    id: str
    name: str


@dataclass(frozen=True)
class Info:
    """Information about the running workflow (subset of temporalio's
    ``workflow.Info``).
    """

    attempt: int
    # The previous run of this chain when this run was created by a continuation
    # (continue-as-new, a workflow retry, or a cron continuation), else None.
    continued_run_id: Optional[str] = None
    cron_schedule: Optional[str] = None
    # Whole-execution (run-chain) timeout: accepted but not enforced
    # (ARCHITECTURE start-params). Surfaced for parity; always None.
    execution_timeout: Optional[timedelta] = None
    # The run id of the first execution in this run chain (run 0's DBOS id =
    # the Temporal workflow id). Derived from our run-chain id scheme.
    first_execution_run_id: str = ""
    # The run's interceptor headers, decoded to Payloads (the same mapping
    # surfaced to workflow interceptors as ExecuteWorkflowInput.headers).
    headers: Mapping[str, "Payload"] = field(default_factory=dict)
    namespace: str = "default"
    # The parent workflow, when started cross-chain as a child; None otherwise.
    parent: Optional[ParentInfo] = None
    # The root workflow of this run's tree; None for a top-level workflow (which
    # is itself the root). Threaded through child starts.
    root: Optional[RootInfo] = None
    # Priority is accepted-and-inert (DBOS queues are FIFO); always the default
    # instance, as temporalio returns for an unset priority.
    priority: Priority = Priority.default
    retry_policy: Optional[RetryPolicy] = None
    run_id: str = ""
    run_timeout: Optional[timedelta] = None
    search_attributes: SearchAttributes = field(default_factory=dict)
    """Search attributes for the workflow.

    .. deprecated::
        Use :py:attr:`typed_search_attributes` instead.
    """
    start_time: datetime = datetime.fromtimestamp(0, timezone.utc)
    task_queue: str = ""
    # Workflow-task timeout: no workflow-task concept here (inert). Surfaced for
    # parity; always None.
    task_timeout: Optional[timedelta] = None
    typed_search_attributes: TypedSearchAttributes = TypedSearchAttributes.empty
    workflow_id: str = ""
    # The run's initialization time. A single start timestamp per run (no
    # "first task" vs "initialization" distinction), so equals start_time.
    workflow_start_time: datetime = datetime.fromtimestamp(0, timezone.utc)
    workflow_type: str = ""

    def get_current_history_length(self) -> int:
        """Approximated as the run's checkpoint cursor (claimed DBOS
        function ids) — the analog of history events here."""
        return _runtime().runtime_history_length()

    def get_current_history_size(self) -> int:
        """History byte size is not tracked; always 0. Use
        :py:meth:`is_continue_as_new_suggested` (threshold on checkpoint
        count) for continue-as-new decisions."""
        return 0

    def is_continue_as_new_suggested(self) -> bool:
        """Whether this run's checkpoint count has passed the
        continue-as-new suggestion threshold
        (``DBOSIFY_CAN_SUGGESTION_THRESHOLD``, default 10000)."""
        return _runtime().runtime_can_suggested()

    def get_current_build_id(self) -> str:
        """The build id of the worker executing this run — the DBOS
        ``application_version`` DBOS pins recovery/dequeue to (ARCHITECTURE worker-versioning).
        Empty string when no worker deployment version is set.

        .. warning::
            Read *live* from the executing worker, so it is **not replay-stable**
            across a version change (a re-execution or :py:class:`Replayer` run on
            a newer worker reports that worker's build id). Do not branch workflow
            logic on it — use :py:func:`patched` for versioned code changes.

        .. deprecated::
            Use :py:meth:`get_current_deployment_version` instead.
        """
        version = _runtime().runtime_get_current_deployment_version()
        return version.build_id if version is not None else ""

    def get_current_deployment_version(self) -> Optional[WorkerDeploymentVersion]:
        """The deployment version of the worker executing this run (deployment
        name = DBOS application/deployment name, build id = the DBOS
        ``application_version`` DBOS pins recovery/dequeue to). None when no
        worker deployment version is set (e.g. the in-process dispatcher
        harness). ARCHITECTURE worker-versioning.

        .. warning::
            Read *live* from the executing worker, so it is **not replay-stable**
            across a version change. Do not branch workflow logic on it — use
            :py:func:`patched` for versioned code changes.
        """
        return _runtime().runtime_get_current_deployment_version()

    def is_target_worker_deployment_version_changed(self) -> bool:
        """Whether the target worker deployment version has changed
        (upgrade-on-continue-as-new). Always False in dbosify: workflows
        are pinned to their build id and never auto-upgrade (ARCHITECTURE worker-versioning).
        """
        return False


class _Runtime:
    """Interface the interpreter implements to back this module's functions.

    Defined here (not in _internal) so _internal modules can import it
    without cycles.
    """

    def runtime_info(self) -> Info:
        raise NotImplementedError

    def runtime_outbound(self) -> "WorkflowOutboundInterceptor":
        raise NotImplementedError

    def runtime_now(self) -> float:
        raise NotImplementedError

    def runtime_random(self) -> Random:
        raise NotImplementedError

    def runtime_random_seed(self) -> int:
        raise NotImplementedError

    def runtime_register_random_seed_callback(
        self, callback: Callable[[int], None]
    ) -> None:
        raise NotImplementedError

    def runtime_instance(self) -> Any:
        raise NotImplementedError

    def runtime_is_replaying(self) -> bool:
        raise NotImplementedError

    def runtime_is_read_only(self) -> bool:
        raise NotImplementedError

    def runtime_get_handler(
        self, category: str, name: Optional[str]
    ) -> Optional[Callable[..., Any]]:
        raise NotImplementedError

    def runtime_set_handler(
        self,
        category: str,
        name: Optional[str],
        handler: Optional[Callable[..., Any]],
        validator: Optional[Callable[..., Any]] = None,
    ) -> None:
        raise NotImplementedError

    def runtime_patched(self, id: str) -> bool:
        raise NotImplementedError

    def runtime_deprecate_patch(self, id: str) -> None:
        raise NotImplementedError

    def runtime_history_length(self) -> int:
        raise NotImplementedError

    def runtime_can_suggested(self) -> bool:
        raise NotImplementedError

    def runtime_get_current_deployment_version(
        self,
    ) -> Optional[WorkerDeploymentVersion]:
        raise NotImplementedError

    def runtime_get_current_details(self) -> str:
        raise NotImplementedError

    def runtime_set_current_details(self, details: str) -> None:
        raise NotImplementedError

    def runtime_cancellation_reason(self) -> Optional[str]:
        raise NotImplementedError

    def runtime_all_handlers_finished(self) -> bool:
        raise NotImplementedError

    def runtime_has_last_completion_result(self) -> bool:
        raise NotImplementedError

    def runtime_last_completion_result(self, type_hint: Optional[type] = None) -> Any:
        raise NotImplementedError

    def runtime_last_failure(self) -> Optional[BaseException]:
        raise NotImplementedError

    def runtime_memo(self) -> Mapping[str, Any]:
        raise NotImplementedError

    def runtime_memo_value(self, key: str, *, type_hint: Optional[type] = None) -> Any:
        raise NotImplementedError

    def runtime_upsert_memo(self, updates: Mapping[str, Any]) -> None:
        raise NotImplementedError

    def runtime_upsert_search_attributes(
        self,
        attributes: Union[SearchAttributes, Sequence[SearchAttributeUpdate[Any]]],
    ) -> None:
        raise NotImplementedError

    def runtime_start_activity(
        self,
        activity_name: str,
        args: Sequence[Any],
        *,
        schedule_to_close_timeout: Optional[timedelta],
        start_to_close_timeout: Optional[timedelta],
        retry_policy: Optional[RetryPolicy],
        activity_id: Optional[str],
        cancellation_type: int = 0,
        heartbeat_timeout: Optional[timedelta] = None,
        result_type: Optional[type] = None,
        task_queue: Optional[str] = None,
        schedule_to_start_timeout: Optional[timedelta] = None,
        headers: Optional[Mapping[str, Any]] = None,
    ) -> "ActivityHandle":
        raise NotImplementedError

    async def runtime_wait_condition(
        self, fn: Callable[[], bool], *, timeout: Optional[float]
    ) -> None:
        raise NotImplementedError

    async def runtime_start_child_workflow(
        self,
        type_name: str,
        args: Sequence[Any],
        *,
        child_id: Optional[str],
        task_queue: Optional[str],
        parent_close_policy: int,
        cancellation_type: int,
        memo: Optional[Mapping[str, Any]] = None,
        search_attributes: Optional[
            Union[TypedSearchAttributes, SearchAttributes]
        ] = None,
        headers: Optional[Mapping[str, Any]] = None,
    ) -> "ChildWorkflowHandle":
        raise NotImplementedError

    async def runtime_send_to_workflow(
        self, workflow_id: str, envelope: Any, *, resolve_chain: bool = False
    ) -> None:
        raise NotImplementedError


class ActivityHandle:
    """Handle to a started activity: awaitable for its result. Cancellation
    reaches it implicitly (workflow cancel, ``wait_for`` timeouts) or via
    :py:meth:`cancel`, honoring the activity's ``cancellation_type``.
    """

    def __init__(
        self,
        future: "asyncio.Future[Any]",
        on_cancel: Optional[Callable[[], None]] = None,
    ) -> None:
        self._future = future
        self._on_cancel = on_cancel

    def __await__(self) -> Any:
        return self._future.__await__()

    def done(self) -> bool:
        return self._future.done()

    def result(self) -> Any:
        return self._future.result()

    def cancel(self, msg: Optional[Any] = None) -> bool:
        """Request cancellation of the activity. With
        WAIT_CANCELLATION_COMPLETED the await resolves only once the
        activity has observed the request and unwound; the default
        TRY_CANCEL resolves immediately.
        """
        if self._on_cancel is not None:
            self._on_cancel()
            return True
        return self._future.cancel(msg)


class ChildWorkflowHandle:
    """Handle to a started child workflow: awaitable for its result, plus
    ``signal`` (checkpointed send from the parent's perspective), ``cancel``,
    and a synchronous ``result`` mirroring temporalio's Task-based handle.
    """

    def __init__(
        self, runtime: "_Runtime", workflow_id: str, future: "asyncio.Future[Any]"
    ) -> None:
        self._runtime = runtime
        self._id = workflow_id
        self._future = future

    @property
    def id(self) -> str:
        """ID of the child workflow."""
        return self._id

    @property
    def first_execution_run_id(self) -> Optional[str]:
        """Run ID of the child's first run (its DBOS workflow id)."""
        return self._id

    def __await__(self) -> Any:
        return self._future.__await__()

    def done(self) -> bool:
        return self._future.done()

    def result(self) -> Any:
        """The child's result if it has completed, else raise
        ``InvalidStateError`` (Task semantics). Usually you ``await`` the
        handle instead; this mirrors temporalio's synchronous accessor."""
        return self._future.result()

    def cancel(self, msg: Optional[Any] = None) -> bool:
        """Request cancellation of the child workflow, honoring its
        ``ChildWorkflowCancellationType``: the cooperative-cancel envelope is
        delivered to the child's current run at the next event boundary (ABANDON
        just retires the waiter). Returns ``False`` if the child is already
        done. Equivalent to cancelling the awaited handle."""
        return self._future.cancel(msg)

    async def signal(
        self, signal: Any, arg: Any = _arg_unset, *, args: Sequence[Any] = []
    ) -> None:
        """Send a signal to the child workflow."""
        from ._internal.workflow_interceptor import SignalChildWorkflowInput

        name = (
            signal
            if isinstance(signal, str)
            else getattr(signal, _registry.SIGNAL_ATTR)
        )
        # The outbound root resolves the chain so the signal reaches the child's
        # *current* run (replay-safe: the checkpointed send resolves once).
        input = SignalChildWorkflowInput(
            signal=str(name),
            args=_resolve_args(arg, args),
            child_workflow_id=self._id,
            headers={},
        )
        await self._runtime.runtime_outbound().signal_child_workflow(input)


def _runtime() -> _Runtime:
    try:
        loop = asyncio.get_running_loop()
    except RuntimeError:
        loop = None
    runtime = getattr(loop, "dbosify_runtime", None)
    if runtime is None:
        raise RuntimeError("Not in workflow event loop")
    assert isinstance(runtime, _Runtime)
    return runtime


def in_workflow() -> bool:
    """Whether the current code is inside a workflow."""
    try:
        _runtime()
        return True
    except RuntimeError:
        return False


# ---------------------------------------------------------------------------
# Runtime functions
# ---------------------------------------------------------------------------


def info() -> Info:
    """Current workflow's info."""
    return _runtime().runtime_outbound().info()


def payload_converter() -> PayloadConverter:
    """The payload converter for this workflow (the process's active
    ``DataConverter``'s payload converter), mirroring
    ``temporalio.workflow.payload_converter``.

    Use it to convert the ``RawValue`` arguments a dynamic handler receives
    (``payload_converter().from_payload(arg.payload, MyType)``) or to
    encode/decode interceptor header values (``to_payload``/``from_payload``).
    """
    from ._internal import conversion

    return conversion.get_converter().payload_converter


def all_handlers_finished() -> bool:
    """Whether all in-progress signal/update handlers have finished. Used in
    wait_condition predicates to avoid returning while handlers still run.
    """
    return _runtime().runtime_all_handlers_finished()


# --- runtime handler accessors -----------------------------------------------
# Imperative alternative to the decorators: a set overrides any same-name/catch-all handler, None unsets, and a set signal handler drains buffered signals.


def get_signal_handler(name: str) -> Optional[Callable[..., Any]]:
    """Get the signal handler for ``name`` if any."""
    return _runtime().runtime_get_handler("signal", name)


def set_signal_handler(name: str, handler: Optional[Callable[..., Any]]) -> None:
    """Set or unset the signal handler for ``name``. Overrides any handler
    (including a ``@workflow.signal`` one); when set, all unhandled past signals
    for ``name`` are immediately delivered to it."""
    _runtime().runtime_set_handler("signal", name, handler)


def get_dynamic_signal_handler() -> Optional[Callable[..., Any]]:
    """Get the dynamic (catch-all) signal handler if any."""
    return _runtime().runtime_get_handler("signal", None)


def set_dynamic_signal_handler(handler: Optional[Callable[..., Any]]) -> None:
    """Set or unset the dynamic (catch-all) signal handler. When set, all
    unhandled past signals are immediately delivered to it."""
    _runtime().runtime_set_handler("signal", None, handler)


def get_query_handler(name: str) -> Optional[Callable[..., Any]]:
    """Get the query handler for ``name`` if any."""
    return _runtime().runtime_get_handler("query", name)


def set_query_handler(name: str, handler: Optional[Callable[..., Any]]) -> None:
    """Set or unset the query handler for ``name`` (overrides any handler,
    including a ``@workflow.query`` one)."""
    _runtime().runtime_set_handler("query", name, handler)


def get_dynamic_query_handler() -> Optional[Callable[..., Any]]:
    """Get the dynamic (catch-all) query handler if any."""
    return _runtime().runtime_get_handler("query", None)


def set_dynamic_query_handler(handler: Optional[Callable[..., Any]]) -> None:
    """Set or unset the dynamic (catch-all) query handler."""
    _runtime().runtime_set_handler("query", None, handler)


def get_update_handler(name: str) -> Optional[Callable[..., Any]]:
    """Get the update handler for ``name`` if any."""
    return _runtime().runtime_get_handler("update", name)


def set_update_handler(
    name: str,
    handler: Optional[Callable[..., Any]],
    *,
    validator: Optional[Callable[..., Any]] = None,
) -> None:
    """Set or unset the update handler for ``name`` (overrides any handler,
    including a ``@workflow.update`` one), optionally with a ``validator``."""
    _runtime().runtime_set_handler("update", name, handler, validator=validator)


def get_dynamic_update_handler() -> Optional[Callable[..., Any]]:
    """Get the dynamic (catch-all) update handler if any."""
    return _runtime().runtime_get_handler("update", None)


def set_dynamic_update_handler(
    handler: Optional[Callable[..., Any]],
    *,
    validator: Optional[Callable[..., Any]] = None,
) -> None:
    """Set or unset the dynamic (catch-all) update handler, optionally with a
    ``validator``."""
    _runtime().runtime_set_handler("update", None, handler, validator=validator)


def cancellation_reason() -> Optional[str]:
    """The reason for the workflow's cancellation request, if any."""
    return _runtime().runtime_cancellation_reason()


def get_current_details() -> str:
    """The current details of the workflow (free-form, Temporal-markdown,
    multi-line) which may appear in the UI/CLI, mirroring
    ``temporalio.workflow.get_current_details``.

    Unlike static details set at start, this value can be updated throughout
    the life of the workflow via :py:func:`set_current_details`. It is in-memory
    workflow state — reconstructed deterministically on recovery by replaying
    the same :py:func:`set_current_details` calls — and is not surfaced to
    ``describe()``/``list_workflows`` in v1 (ARCHITECTURE current-details). Empty string if
    never set.
    """
    return _runtime().runtime_get_current_details()


def set_current_details(description: str) -> None:
    """Set the current details of the workflow which may appear in the UI/CLI,
    mirroring ``temporalio.workflow.set_current_details``. See
    :py:func:`get_current_details`.
    """
    _runtime().runtime_set_current_details(description)


def has_last_completion_result() -> bool:
    """Whether a previous run of this (cron) workflow chain completed
    successfully — distinguishes "no previous completion" from "the previous
    result was None"."""
    return _runtime().runtime_has_last_completion_result()


def get_last_completion_result(type_hint: Optional[type] = None) -> Any:
    """The result of the chain's last successful run (carried forward across
    failed runs, as in Temporal); None if there was no previous completion
    or the result was None — use :py:func:`has_last_completion_result` to
    tell them apart. ``type_hint`` rebuilds the original type (else a plain
    JSON value, as in temporalio).
    """
    return _runtime().runtime_last_completion_result(type_hint)


def get_last_failure() -> Optional[BaseException]:
    """The failure of this chain's previous run, if it failed — what a
    workflow-retry attempt (or the cron run after a failure) sees."""
    return _runtime().runtime_last_failure()


def memo() -> Mapping[str, Any]:
    """Current workflow's memo values, converted without type hints."""
    return _runtime().runtime_memo()


def memo_value(
    key: str, default: Any = _arg_unset, *, type_hint: Optional[type] = None
) -> Any:
    """Memo value for the given key, optionally rebuilt to ``type_hint``.

    Raises ``KeyError`` if the key is absent and no ``default`` is given.
    """
    # Check presence first, so a KeyError raised while converting to
    # ``type_hint`` propagates instead of being swallowed into ``default``.
    runtime = _runtime()
    if key not in runtime.runtime_memo():
        if default is _arg_unset:
            raise KeyError(f"Memo does not have a value for key {key}")
        return default
    return runtime.runtime_memo_value(key, type_hint=type_hint)


def upsert_memo(updates: Mapping[str, Any]) -> None:
    """Add, modify, and/or remove memo values, with upsert semantics. A value
    of ``None`` removes that key."""
    _runtime().runtime_upsert_memo(updates)


def upsert_search_attributes(
    attributes: Union[SearchAttributes, Sequence[SearchAttributeUpdate[Any]]],
) -> None:
    """Upsert search attributes for this workflow.

    Args:
        attributes: A sequence of updates (created via ``value_set`` /
            ``value_unset`` on search-attribute keys). The dictionary form is
            DEPRECATED.
    """
    _warn_on_deprecated_search_attributes(attributes)
    _runtime().runtime_upsert_search_attributes(attributes)


def now() -> datetime:
    """Current workflow time: deterministic, advances only on events.

    Returns a timezone-aware UTC datetime, matching temporalio (whose
    ``workflow.now()`` documents UTC as the set time zone)."""
    return datetime.fromtimestamp(time(), timezone.utc)


def time() -> float:
    """Current workflow time as seconds since the epoch."""
    return _runtime().runtime_now()


def time_ns() -> int:
    """Current workflow time as nanoseconds since the epoch."""
    return int(_runtime().runtime_now() * 1e9)


def random() -> Random:
    """Deterministically-seeded random instance for this workflow."""
    runtime = _runtime()
    if runtime.runtime_is_read_only():
        # Consuming the shared RNG mutates run state; forbidden in queries/validators.
        raise ReadOnlyContextError("Cannot use random in a read-only context")
    return runtime.runtime_random()


def random_seed() -> int:
    """The seed of this workflow's deterministic random number generator
    (checkpointed once per run), mirroring ``temporalio.workflow.random_seed``."""
    return _runtime().runtime_random_seed()


def register_random_seed_callback(callback: Callable[[int], None]) -> None:
    """Register a callback invoked when the workflow's random seed changes,
    mirroring ``temporalio.workflow.register_random_seed_callback``. In
    dbosify the seed is fixed for a run's lifetime (it never changes
    mid-run), so the callback is stored but never invoked (ARCHITECTURE random-seed)."""
    _runtime().runtime_register_random_seed_callback(callback)


def new_random() -> Random:
    """A new ``Random`` seeded from the current workflow seed and registered to
    reseed when the workflow seed changes, mirroring
    ``temporalio.workflow.new_random``. (The reseed never fires here — see
    :py:func:`register_random_seed_callback`.)"""
    auto_random = Random(random_seed())
    register_random_seed_callback(auto_random.seed)
    return auto_random


def instance() -> Any:
    """The currently running workflow instance (``self``), mirroring
    ``temporalio.workflow.instance``."""
    return _runtime().runtime_instance()


def current_update_info() -> Optional[UpdateInfo]:
    """Info about the update currently being handled (id + name), or ``None``
    when not inside an update handler/validator, mirroring
    ``temporalio.workflow.current_update_info``."""
    return _current_update_info.get(None)


def uuid4() -> uuid_mod.UUID:
    """Deterministic UUID v4 derived from the workflow's random seed."""
    return uuid_mod.UUID(bytes=random().getrandbits(128).to_bytes(16, "big"), version=4)


def patched(id: str) -> bool:
    """Patch a workflow.

    When called, this will only return true if code should take the newer path
    which means this is either not replaying or is replaying and has seen this
    patch before.

    Backed by a durable checkpoint marker: the first non-replaying
    execution records the marker and takes the newer path; a run replaying
    history that predates the patch finds no marker and takes the older path.

    Returns:
        True if this should take the newer path, false if it should take the
        older path.
    """
    return _runtime().runtime_patched(id)


def deprecate_patch(id: str) -> None:
    """Mark a patch as deprecated.

    This marks a workflow that had :py:func:`patched` in a previous version of
    the code as no longer applicable because all workflows that use the old code
    path are done and will never be queried again. Therefore the old code path
    is removed as well.

    Args:
        id: The identifier originally used with :py:func:`patched`.
    """
    _runtime().runtime_deprecate_patch(id)


async def sleep(
    duration: Union[float, timedelta], *, summary: Optional[str] = None
) -> None:
    """Sleep for the given duration on the deterministic loop (a durable
    timer). ``summary`` is accepted and ignored.
    """
    seconds = duration.total_seconds() if isinstance(duration, timedelta) else duration
    await asyncio.sleep(seconds)


async def wait_condition(
    fn: Callable[[], bool],
    *,
    timeout: Optional[Union[float, timedelta]] = None,
    timeout_summary: Optional[str] = None,
) -> None:
    """Wait on a callback to become true, re-evaluated on each new event.
    Raises ``asyncio.TimeoutError`` on timeout (which, uncaught, fails the
    workflow — Temporal semantics).
    """
    seconds = timeout.total_seconds() if isinstance(timeout, timedelta) else timeout
    await _runtime().runtime_wait_condition(fn, timeout=seconds)


def _resolve_activity_name(activity: Any) -> str:
    if isinstance(activity, str):
        return activity
    return _registry.activity_definition_of(activity).name


def _resolve_args(arg: Any, args: Sequence[Any]) -> List[Any]:
    if arg is not _arg_unset:
        if args:
            raise ValueError("Cannot have both arg and args")
        return [arg]
    return list(args)


def start_activity(
    activity: Any,
    arg: Any = _arg_unset,
    *,
    args: Sequence[Any] = [],
    task_queue: Optional[str] = None,
    result_type: Optional[type] = None,
    schedule_to_close_timeout: Optional[timedelta] = None,
    schedule_to_start_timeout: Optional[timedelta] = None,
    start_to_close_timeout: Optional[timedelta] = None,
    heartbeat_timeout: Optional[timedelta] = None,
    retry_policy: Optional[RetryPolicy] = None,
    cancellation_type: Optional[Any] = None,
    activity_id: Optional[str] = None,
    versioning_intent: Optional[Any] = None,
    summary: Optional[str] = None,
    priority: Optional[Any] = None,
) -> ActivityHandle:
    """Start an activity and return its handle.

    Honors arg/args, ``start_to_close_timeout``, ``schedule_to_close_timeout``,
    ``heartbeat_timeout`` (a non-heartbeating attempt fails with
    ``TimeoutType.HEARTBEAT`` and retries), ``retry_policy``,
    ``cancellation_type``, ``activity_id``, ``task_queue`` (when it differs
    from the workflow's own queue the activity runs on another worker via the
    cross-queue path), and ``schedule_to_start_timeout`` (bounds the
    queue dwell on the cross-queue path; a no-op on the local path, which has no
    queue wait); the remaining parameters are accepted and ignored (debug-logged).
    ``result_type``, when given, is the type hint used to reconstruct the
    activity's result (overriding the registered activity's return
    annotation); without it the registry's return type is used, and absent
    both the result decodes hint-free (a plain dict for JSON objects).
    """
    if not start_to_close_timeout and not schedule_to_close_timeout:
        raise ValueError(
            "Activity must have start_to_close_timeout or schedule_to_close_timeout"
        )
    ignored = {
        "versioning_intent": versioning_intent,
        "summary": summary,
        "priority": priority,
    }
    for key, value in ignored.items():
        if value is not None:
            logger.debug("start_activity: ignoring unsupported parameter %r", key)
    from ._internal.workflow_interceptor import StartActivityInput

    input = StartActivityInput(
        activity=_resolve_activity_name(activity),
        args=_resolve_args(arg, args),
        activity_id=activity_id,
        task_queue=task_queue,
        schedule_to_close_timeout=schedule_to_close_timeout,
        schedule_to_start_timeout=schedule_to_start_timeout,
        start_to_close_timeout=start_to_close_timeout,
        heartbeat_timeout=heartbeat_timeout,
        retry_policy=retry_policy,
        cancellation_type=int(
            cancellation_type
            if cancellation_type is not None
            else ActivityCancellationType.TRY_CANCEL
        ),
        headers={},
        disable_eager_execution=False,
        versioning_intent=versioning_intent,
        summary=summary,
        priority=priority,
        arg_types=None,
        ret_type=result_type,
    )
    return _runtime().runtime_outbound().start_activity(input)


async def execute_activity(
    activity: Any,
    arg: Any = _arg_unset,
    *,
    args: Sequence[Any] = [],
    task_queue: Optional[str] = None,
    result_type: Optional[type] = None,
    schedule_to_close_timeout: Optional[timedelta] = None,
    schedule_to_start_timeout: Optional[timedelta] = None,
    start_to_close_timeout: Optional[timedelta] = None,
    heartbeat_timeout: Optional[timedelta] = None,
    retry_policy: Optional[RetryPolicy] = None,
    cancellation_type: Optional[Any] = None,
    activity_id: Optional[str] = None,
    versioning_intent: Optional[Any] = None,
    summary: Optional[str] = None,
    priority: Optional[Any] = None,
) -> Any:
    """Start an activity and wait for completion. See ``start_activity``."""
    return await start_activity(
        activity,
        arg,
        args=args,
        task_queue=task_queue,
        result_type=result_type,
        schedule_to_close_timeout=schedule_to_close_timeout,
        schedule_to_start_timeout=schedule_to_start_timeout,
        start_to_close_timeout=start_to_close_timeout,
        heartbeat_timeout=heartbeat_timeout,
        retry_policy=retry_policy,
        cancellation_type=cancellation_type,
        activity_id=activity_id,
        versioning_intent=versioning_intent,
        summary=summary,
        priority=priority,
    )


def as_completed(
    fs: "Iterable[Awaitable[Any]]", *, timeout: Optional[float] = None
) -> "Iterator[Awaitable[Any]]":
    """Return an iterator whose values are coroutines.

    This is a deterministic version of :py:func:`asyncio.as_completed` (the
    stdlib one iterates sets, whose order varies between processes). Adapted
    from temporalio's, itself adapted from CPython (both MIT).
    """
    if asyncio.isfuture(fs) or asyncio.iscoroutine(fs):
        raise TypeError(f"expect an iterable of futures, not {type(fs).__name__}")

    done: "asyncio.Queue[Optional[asyncio.Future[Any]]]" = asyncio.Queue()

    loop = asyncio.get_event_loop()
    todo: "List[asyncio.Future[Any]]" = [
        asyncio.ensure_future(f, loop=loop) for f in list(fs)
    ]
    timeout_handle = None

    def _on_timeout() -> None:
        for f in todo:
            f.remove_done_callback(_on_completion)
            done.put_nowait(None)  # Queue a dummy value for _wait_for_one().
        todo.clear()  # Can't do todo.remove(f) in the loop.

    def _on_completion(f: "asyncio.Future[Any]") -> None:
        if not todo:
            return  # _on_timeout() was here first.
        todo.remove(f)
        done.put_nowait(f)
        if not todo and timeout_handle is not None:
            timeout_handle.cancel()

    async def _wait_for_one() -> Any:
        f = await done.get()
        if f is None:
            # Dummy value from _on_timeout().
            raise asyncio.TimeoutError
        return f.result()  # May raise f.exception().

    for f in todo:
        f.add_done_callback(_on_completion)
    if todo and timeout is not None:
        timeout_handle = loop.call_later(timeout, _on_timeout)
    for _ in range(len(todo)):
        yield _wait_for_one()


async def wait(
    fs: "Iterable[Any]",
    *,
    timeout: Optional[float] = None,
    return_when: str = asyncio.ALL_COMPLETED,
) -> "Tuple[Any, Any]":
    """Wait for the Futures or Tasks given by fs to complete.

    This is a deterministic version of :py:func:`asyncio.wait`: done and
    pending are *lists in input order*, not sets (whose iteration order
    varies between processes — replay poison). Adapted from temporalio's,
    itself adapted from CPython (both MIT).
    """
    if asyncio.isfuture(fs) or asyncio.iscoroutine(fs):
        raise TypeError(f"Expect an iterable of Tasks/Futures, not {type(fs).__name__}")
    if not fs:
        raise ValueError("Sequence of Tasks/Futures must not be empty.")
    if return_when not in (
        asyncio.FIRST_COMPLETED,
        asyncio.FIRST_EXCEPTION,
        asyncio.ALL_COMPLETED,
    ):
        raise ValueError(f"Invalid return_when value: {return_when}")

    fs_list = list(fs)

    if any(asyncio.iscoroutine(f) for f in fs_list):
        raise TypeError("Passing coroutines is forbidden, use tasks explicitly.")

    loop = asyncio.get_running_loop()
    waiter: "asyncio.Future[None]" = loop.create_future()
    timeout_handle = None
    if timeout is not None:
        timeout_handle = loop.call_later(timeout, _release_waiter, waiter)
    counter = len(fs_list)

    def _on_completion(f: "asyncio.Future[Any]") -> None:
        nonlocal counter
        counter -= 1
        if (
            counter <= 0
            or return_when == asyncio.FIRST_COMPLETED
            or return_when == asyncio.FIRST_EXCEPTION
            and (not f.cancelled() and f.exception() is not None)
        ):
            if timeout_handle is not None:
                timeout_handle.cancel()
            if not waiter.done():
                waiter.set_result(None)

    for f in fs_list:
        f.add_done_callback(_on_completion)

    try:
        await waiter
    finally:
        if timeout_handle is not None:
            timeout_handle.cancel()
        for f in fs_list:
            f.remove_done_callback(_on_completion)

    done, pending = [], []
    for f in fs_list:
        if f.done():
            done.append(f)
        else:
            pending.append(f)
    return done, pending


def _release_waiter(waiter: "asyncio.Future[Any]", *_args: Any) -> None:
    if not waiter.done():
        waiter.set_result(None)


class ContinueAsNewError(BaseException):
    """Thrown by :py:func:`continue_as_new`; must escape the run method
    uncaught (mirrors temporalio: a ``BaseException`` so bare ``except
    Exception`` blocks don't swallow it)."""

    def __init__(self, *args: object) -> None:
        super().__init__(*args)
        self._dbosify_args: Sequence[Any] = ()
        self._dbosify_workflow: Optional[str] = None
        self._dbosify_task_queue: Optional[str] = None
        self._dbosify_run_timeout: Optional[timedelta] = None
        self._dbosify_retry_policy: Optional[RetryPolicy] = None
        self._dbosify_memo: Optional[Mapping[str, Any]] = None
        self._dbosify_search_attributes: Optional[
            Union[TypedSearchAttributes, SearchAttributes]
        ] = None
        # Interceptor headers for the new run, in wire form (set by the outbound
        # chain root); the chain's carried headers are dropped unless re-injected.
        self._dbosify_headers: Optional[Dict[str, Any]] = None


class NondeterminismError(exceptions.TemporalError):
    """Error thrown during replay when workflow code diverges from the
    recorded history (mirrors ``temporalio.workflow.NondeterminismError``)."""

    def __init__(self, message: str) -> None:
        super().__init__(message)
        self.message = message


class ReadOnlyContextError(exceptions.TemporalError):
    """Raised when workflow code attempts a state-mutating operation from a
    read-only context (a query handler or update validator), mirroring
    ``temporalio.workflow.ReadOnlyContextError``."""

    def __init__(self, message: str) -> None:
        super().__init__(message)
        self.message = message


def continue_as_new(
    arg: Any = _arg_unset,
    *,
    args: Sequence[Any] = [],
    workflow: Any = None,
    task_queue: Optional[str] = None,
    run_timeout: Optional[timedelta] = None,
    task_timeout: Optional[timedelta] = None,
    retry_policy: Optional[RetryPolicy] = None,
    memo: Optional[Any] = None,
    search_attributes: Optional[Any] = None,
    versioning_intent: Optional[Any] = None,
    initial_versioning_behavior: Optional[ContinueAsNewVersioningBehavior] = None,
) -> "NoReturn":
    """Stop the current run and continue the chain as a new run with the
    given arguments (same workflow type unless ``workflow`` is given). The
    raised :py:class:`ContinueAsNewError` must not be caught.

    ``versioning_intent``/``initial_versioning_behavior`` are accepted for
    parity; the new run is pinned to the enqueuing worker's build ID, and the
    auto-upgrade/ramping variants have no DBOS analog (ARCHITECTURE worker-versioning).
    """
    if _runtime().runtime_is_read_only():
        raise ReadOnlyContextError("Cannot continue-as-new in a read-only context")
    for key, value in {
        "task_timeout": task_timeout,
        "versioning_intent": versioning_intent,
        "initial_versioning_behavior": initial_versioning_behavior,
    }.items():
        if value is not None:
            logger.debug("continue_as_new: ignoring unsupported parameter %r", key)
    _warn_on_deprecated_search_attributes(search_attributes)
    if retry_policy is not None:
        retry_policy._validate()
    from ._internal.workflow_interceptor import ContinueAsNewInput

    input = ContinueAsNewInput(
        workflow=_resolve_workflow_type(workflow) if workflow is not None else None,
        args=_resolve_args(arg, args),
        task_queue=task_queue,
        # Overrides for the new run; absent, the chain's carried values apply.
        run_timeout=run_timeout,
        task_timeout=task_timeout,
        retry_policy=retry_policy,
        memo=memo,
        search_attributes=search_attributes,
        headers={},
        versioning_intent=versioning_intent,
        initial_versioning_behavior=initial_versioning_behavior,
        arg_types=None,
    )
    # Routes through the outbound chain and raises ContinueAsNewError (NoReturn).
    _runtime().runtime_outbound().continue_as_new(input)


def _resolve_workflow_type(workflow: Any) -> str:
    """Resolve a workflow reference: the class, its run method, or a name."""
    if isinstance(workflow, str):
        return workflow
    if isinstance(workflow, type):
        return _registry.workflow_definition_of(workflow).name
    name = getattr(workflow, _registry.WORKFLOW_NAME_ATTR, None)
    if name is not None:
        return str(name)
    raise TypeError(
        f"Cannot resolve a workflow type from {workflow!r}: pass the "
        "@workflow.defn class, its @workflow.run method, or the type name"
    )


async def start_child_workflow(
    workflow: Any,
    arg: Any = _arg_unset,
    *,
    args: Sequence[Any] = [],
    id: Optional[str] = None,
    task_queue: Optional[str] = None,
    result_type: Optional[type] = None,
    cancellation_type: ChildWorkflowCancellationType = ChildWorkflowCancellationType.WAIT_CANCELLATION_COMPLETED,
    parent_close_policy: ParentClosePolicy = ParentClosePolicy.TERMINATE,
    execution_timeout: Optional[timedelta] = None,
    run_timeout: Optional[timedelta] = None,
    task_timeout: Optional[timedelta] = None,
    id_reuse_policy: Optional[Any] = None,
    retry_policy: Optional[RetryPolicy] = None,
    cron_schedule: str = "",
    memo: Optional[Any] = None,
    search_attributes: Optional[Any] = None,
    versioning_intent: Optional[Any] = None,
    static_summary: Optional[str] = None,
    static_details: Optional[str] = None,
    priority: Optional[Any] = None,
) -> ChildWorkflowHandle:
    """Start a child workflow; returns its handle once the start is durable
    (Temporal semantics: resolves on start, not completion).

    Honors arg/args, id (default: ``{parent_id}_{seq}``), task_queue,
    parent_close_policy, cancellation_type, and (matching top-level starts)
    ``run_timeout`` and ``retry_policy``. ``cron_schedule`` and
    ``id_reuse_policy`` are accepted-but-pending for children
    (ARCHITECTURE start-params); the remaining parameters are accepted and ignored
    (debug-logged).
    """
    for key, value in {
        "result_type": result_type,
        "execution_timeout": execution_timeout,
        "task_timeout": task_timeout,
        "id_reuse_policy": id_reuse_policy,
        "cron_schedule": cron_schedule or None,
        "versioning_intent": versioning_intent,
        "static_summary": static_summary,
        "static_details": static_details,
        "priority": priority,
    }.items():
        if value is not None:
            logger.debug("start_child_workflow: ignoring unsupported option %r", key)
    _warn_on_deprecated_search_attributes(search_attributes)
    from ._internal import ids as _ids
    from ._internal.workflow_interceptor import StartChildWorkflowInput

    # Explicit id is validated here; a None id stays auto (interpreter derives
    # ``{parent}_{seq}``, carried as "" since input.id is a non-optional str).
    if id is not None:
        _ids.validate_workflow_id(id)
    input = StartChildWorkflowInput(
        workflow=_resolve_workflow_type(workflow),
        args=_resolve_args(arg, args),
        id=id or "",
        task_queue=task_queue,
        cancellation_type=cancellation_type,
        parent_close_policy=parent_close_policy,
        execution_timeout=execution_timeout,
        run_timeout=run_timeout,
        task_timeout=task_timeout,
        id_reuse_policy=id_reuse_policy,
        retry_policy=retry_policy,
        cron_schedule=cron_schedule,
        memo=memo,
        search_attributes=search_attributes,
        headers={},
        versioning_intent=versioning_intent,
        static_summary=static_summary,
        static_details=static_details,
        priority=priority,
        arg_types=None,
        ret_type=result_type,
    )
    return await _runtime().runtime_outbound().start_child_workflow(input)


def get_external_workflow_handle(
    workflow_id: str, *, run_id: Optional[str] = None
) -> ExternalWorkflowHandle:
    """Get a handle to an external workflow for signalling/cancelling. With
    no ``run_id``, operations target the id's current run.
    """
    return ExternalWorkflowHandle(_runtime(), workflow_id, run_id)


def get_external_workflow_handle_for(
    workflow: Any, workflow_id: str, *, run_id: Optional[str] = None
) -> ExternalWorkflowHandle:
    """Typed variant of :py:func:`get_external_workflow_handle`."""
    return get_external_workflow_handle(workflow_id, run_id=run_id)


async def execute_child_workflow(
    workflow: Any,
    arg: Any = _arg_unset,
    *,
    args: Sequence[Any] = [],
    id: Optional[str] = None,
    task_queue: Optional[str] = None,
    result_type: Optional[type] = None,
    cancellation_type: ChildWorkflowCancellationType = ChildWorkflowCancellationType.WAIT_CANCELLATION_COMPLETED,
    parent_close_policy: ParentClosePolicy = ParentClosePolicy.TERMINATE,
    execution_timeout: Optional[timedelta] = None,
    run_timeout: Optional[timedelta] = None,
    task_timeout: Optional[timedelta] = None,
    id_reuse_policy: Optional[Any] = None,
    retry_policy: Optional[RetryPolicy] = None,
    cron_schedule: str = "",
    memo: Optional[Any] = None,
    search_attributes: Optional[Any] = None,
    versioning_intent: Optional[Any] = None,
    static_summary: Optional[str] = None,
    static_details: Optional[str] = None,
    priority: Optional[Any] = None,
) -> Any:
    """Start a child workflow and wait for its result. See
    ``start_child_workflow``.
    """
    handle = await start_child_workflow(
        workflow,
        arg,
        args=args,
        id=id,
        task_queue=task_queue,
        result_type=result_type,
        cancellation_type=cancellation_type,
        parent_close_policy=parent_close_policy,
        execution_timeout=execution_timeout,
        run_timeout=run_timeout,
        task_timeout=task_timeout,
        id_reuse_policy=id_reuse_policy,
        retry_policy=retry_policy,
        cron_schedule=cron_schedule,
        memo=memo,
        search_attributes=search_attributes,
        versioning_intent=versioning_intent,
        static_summary=static_summary,
        static_details=static_details,
        priority=priority,
    )
    return await handle


# Method variants: identical resolution/execution. Mirroring temporalio, these
# lack ``result_type`` — the return type is inferred from the method.
def start_activity_method(
    activity: Any,
    arg: Any = _arg_unset,
    *,
    args: Sequence[Any] = [],
    task_queue: Optional[str] = None,
    schedule_to_close_timeout: Optional[timedelta] = None,
    schedule_to_start_timeout: Optional[timedelta] = None,
    start_to_close_timeout: Optional[timedelta] = None,
    heartbeat_timeout: Optional[timedelta] = None,
    retry_policy: Optional[RetryPolicy] = None,
    cancellation_type: Optional[Any] = None,
    activity_id: Optional[str] = None,
    versioning_intent: Optional[Any] = None,
    summary: Optional[str] = None,
    priority: Optional[Any] = None,
) -> ActivityHandle:
    """Start an activity from a method reference. See ``start_activity``."""
    return start_activity(
        activity,
        arg,
        args=args,
        task_queue=task_queue,
        schedule_to_close_timeout=schedule_to_close_timeout,
        schedule_to_start_timeout=schedule_to_start_timeout,
        start_to_close_timeout=start_to_close_timeout,
        heartbeat_timeout=heartbeat_timeout,
        retry_policy=retry_policy,
        cancellation_type=cancellation_type,
        activity_id=activity_id,
        versioning_intent=versioning_intent,
        summary=summary,
        priority=priority,
    )


async def execute_activity_method(
    activity: Any,
    arg: Any = _arg_unset,
    *,
    args: Sequence[Any] = [],
    task_queue: Optional[str] = None,
    schedule_to_close_timeout: Optional[timedelta] = None,
    schedule_to_start_timeout: Optional[timedelta] = None,
    start_to_close_timeout: Optional[timedelta] = None,
    heartbeat_timeout: Optional[timedelta] = None,
    retry_policy: Optional[RetryPolicy] = None,
    cancellation_type: Optional[Any] = None,
    activity_id: Optional[str] = None,
    versioning_intent: Optional[Any] = None,
    summary: Optional[str] = None,
    priority: Optional[Any] = None,
) -> Any:
    """Run an activity from a method reference. See ``execute_activity``."""
    return await start_activity_method(
        activity,
        arg,
        args=args,
        task_queue=task_queue,
        schedule_to_close_timeout=schedule_to_close_timeout,
        schedule_to_start_timeout=schedule_to_start_timeout,
        start_to_close_timeout=start_to_close_timeout,
        heartbeat_timeout=heartbeat_timeout,
        retry_policy=retry_policy,
        cancellation_type=cancellation_type,
        activity_id=activity_id,
        versioning_intent=versioning_intent,
        summary=summary,
        priority=priority,
    )


def start_activity_class(
    activity: Any,
    arg: Any = _arg_unset,
    *,
    args: Sequence[Any] = [],
    task_queue: Optional[str] = None,
    schedule_to_close_timeout: Optional[timedelta] = None,
    schedule_to_start_timeout: Optional[timedelta] = None,
    start_to_close_timeout: Optional[timedelta] = None,
    heartbeat_timeout: Optional[timedelta] = None,
    retry_policy: Optional[RetryPolicy] = None,
    cancellation_type: Optional[Any] = None,
    activity_id: Optional[str] = None,
    versioning_intent: Optional[Any] = None,
    summary: Optional[str] = None,
    priority: Optional[Any] = None,
) -> ActivityHandle:
    """Start an activity from a callable-class reference. See ``start_activity``."""
    return start_activity(
        activity,
        arg,
        args=args,
        task_queue=task_queue,
        schedule_to_close_timeout=schedule_to_close_timeout,
        schedule_to_start_timeout=schedule_to_start_timeout,
        start_to_close_timeout=start_to_close_timeout,
        heartbeat_timeout=heartbeat_timeout,
        retry_policy=retry_policy,
        cancellation_type=cancellation_type,
        activity_id=activity_id,
        versioning_intent=versioning_intent,
        summary=summary,
        priority=priority,
    )


async def execute_activity_class(
    activity: Any,
    arg: Any = _arg_unset,
    *,
    args: Sequence[Any] = [],
    task_queue: Optional[str] = None,
    schedule_to_close_timeout: Optional[timedelta] = None,
    schedule_to_start_timeout: Optional[timedelta] = None,
    start_to_close_timeout: Optional[timedelta] = None,
    heartbeat_timeout: Optional[timedelta] = None,
    retry_policy: Optional[RetryPolicy] = None,
    cancellation_type: Optional[Any] = None,
    activity_id: Optional[str] = None,
    versioning_intent: Optional[Any] = None,
    summary: Optional[str] = None,
    priority: Optional[Any] = None,
) -> Any:
    """Run an activity from a callable-class reference. See ``execute_activity``."""
    return await start_activity_class(
        activity,
        arg,
        args=args,
        task_queue=task_queue,
        schedule_to_close_timeout=schedule_to_close_timeout,
        schedule_to_start_timeout=schedule_to_start_timeout,
        start_to_close_timeout=start_to_close_timeout,
        heartbeat_timeout=heartbeat_timeout,
        retry_policy=retry_policy,
        cancellation_type=cancellation_type,
        activity_id=activity_id,
        versioning_intent=versioning_intent,
        summary=summary,
        priority=priority,
    )


def start_local_activity(
    activity: Any,
    arg: Any = _arg_unset,
    *,
    args: Sequence[Any] = [],
    result_type: Optional[type] = None,
    schedule_to_close_timeout: Optional[timedelta] = None,
    schedule_to_start_timeout: Optional[timedelta] = None,
    start_to_close_timeout: Optional[timedelta] = None,
    retry_policy: Optional[RetryPolicy] = None,
    local_retry_threshold: Optional[timedelta] = None,
    cancellation_type: Optional[Any] = None,
    activity_id: Optional[str] = None,
    summary: Optional[str] = None,
) -> ActivityHandle:
    """Start a local activity. In dbosify, in-process step execution
    *is* the local path, so this shares machinery with
    ``start_activity``.
    """
    if not start_to_close_timeout and not schedule_to_close_timeout:
        raise ValueError(
            "Activity must have start_to_close_timeout or schedule_to_close_timeout"
        )
    for key, value in {
        "schedule_to_start_timeout": schedule_to_start_timeout,
        "local_retry_threshold": local_retry_threshold,
        "summary": summary,
    }.items():
        if value is not None:
            logger.debug("start_local_activity: ignoring unsupported parameter %r", key)
    from ._internal.workflow_interceptor import StartLocalActivityInput

    input = StartLocalActivityInput(
        activity=_resolve_activity_name(activity),
        args=_resolve_args(arg, args),
        activity_id=activity_id,
        schedule_to_close_timeout=schedule_to_close_timeout,
        schedule_to_start_timeout=schedule_to_start_timeout,
        start_to_close_timeout=start_to_close_timeout,
        retry_policy=retry_policy,
        local_retry_threshold=local_retry_threshold,
        cancellation_type=int(
            cancellation_type
            if cancellation_type is not None
            else ActivityCancellationType.TRY_CANCEL
        ),
        headers={},
        summary=summary,
        arg_types=None,
        ret_type=result_type,
    )
    return _runtime().runtime_outbound().start_local_activity(input)


async def execute_local_activity(
    activity: Any,
    arg: Any = _arg_unset,
    *,
    args: Sequence[Any] = [],
    result_type: Optional[type] = None,
    schedule_to_close_timeout: Optional[timedelta] = None,
    schedule_to_start_timeout: Optional[timedelta] = None,
    start_to_close_timeout: Optional[timedelta] = None,
    retry_policy: Optional[RetryPolicy] = None,
    local_retry_threshold: Optional[timedelta] = None,
    cancellation_type: Optional[Any] = None,
    activity_id: Optional[str] = None,
    summary: Optional[str] = None,
) -> Any:
    """Start a local activity and wait for completion."""
    return await start_local_activity(
        activity,
        arg,
        args=args,
        result_type=result_type,
        schedule_to_close_timeout=schedule_to_close_timeout,
        schedule_to_start_timeout=schedule_to_start_timeout,
        start_to_close_timeout=start_to_close_timeout,
        retry_policy=retry_policy,
        local_retry_threshold=local_retry_threshold,
        cancellation_type=cancellation_type,
        activity_id=activity_id,
        summary=summary,
    )


def start_local_activity_method(
    activity: Any,
    arg: Any = _arg_unset,
    *,
    args: Sequence[Any] = [],
    schedule_to_close_timeout: Optional[timedelta] = None,
    schedule_to_start_timeout: Optional[timedelta] = None,
    start_to_close_timeout: Optional[timedelta] = None,
    retry_policy: Optional[RetryPolicy] = None,
    local_retry_threshold: Optional[timedelta] = None,
    cancellation_type: Optional[Any] = None,
    activity_id: Optional[str] = None,
    summary: Optional[str] = None,
) -> ActivityHandle:
    """Start a local activity from a method reference (no ``result_type``,
    mirroring temporalio: the return type is inferred from the method)."""
    return start_local_activity(
        activity,
        arg,
        args=args,
        schedule_to_close_timeout=schedule_to_close_timeout,
        schedule_to_start_timeout=schedule_to_start_timeout,
        start_to_close_timeout=start_to_close_timeout,
        retry_policy=retry_policy,
        local_retry_threshold=local_retry_threshold,
        cancellation_type=cancellation_type,
        activity_id=activity_id,
        summary=summary,
    )


async def execute_local_activity_method(
    activity: Any,
    arg: Any = _arg_unset,
    *,
    args: Sequence[Any] = [],
    schedule_to_close_timeout: Optional[timedelta] = None,
    schedule_to_start_timeout: Optional[timedelta] = None,
    start_to_close_timeout: Optional[timedelta] = None,
    retry_policy: Optional[RetryPolicy] = None,
    local_retry_threshold: Optional[timedelta] = None,
    cancellation_type: Optional[Any] = None,
    activity_id: Optional[str] = None,
    summary: Optional[str] = None,
) -> Any:
    """Run a local activity from a method reference."""
    return await start_local_activity_method(
        activity,
        arg,
        args=args,
        schedule_to_close_timeout=schedule_to_close_timeout,
        schedule_to_start_timeout=schedule_to_start_timeout,
        start_to_close_timeout=start_to_close_timeout,
        retry_policy=retry_policy,
        local_retry_threshold=local_retry_threshold,
        cancellation_type=cancellation_type,
        activity_id=activity_id,
        summary=summary,
    )


def start_local_activity_class(
    activity: Any,
    arg: Any = _arg_unset,
    *,
    args: Sequence[Any] = [],
    schedule_to_close_timeout: Optional[timedelta] = None,
    schedule_to_start_timeout: Optional[timedelta] = None,
    start_to_close_timeout: Optional[timedelta] = None,
    retry_policy: Optional[RetryPolicy] = None,
    local_retry_threshold: Optional[timedelta] = None,
    cancellation_type: Optional[Any] = None,
    activity_id: Optional[str] = None,
    summary: Optional[str] = None,
) -> ActivityHandle:
    """Start a local activity from a callable-class reference (no
    ``result_type``, mirroring temporalio: the return type is inferred)."""
    return start_local_activity(
        activity,
        arg,
        args=args,
        schedule_to_close_timeout=schedule_to_close_timeout,
        schedule_to_start_timeout=schedule_to_start_timeout,
        start_to_close_timeout=start_to_close_timeout,
        retry_policy=retry_policy,
        local_retry_threshold=local_retry_threshold,
        cancellation_type=cancellation_type,
        activity_id=activity_id,
        summary=summary,
    )


async def execute_local_activity_class(
    activity: Any,
    arg: Any = _arg_unset,
    *,
    args: Sequence[Any] = [],
    schedule_to_close_timeout: Optional[timedelta] = None,
    schedule_to_start_timeout: Optional[timedelta] = None,
    start_to_close_timeout: Optional[timedelta] = None,
    retry_policy: Optional[RetryPolicy] = None,
    local_retry_threshold: Optional[timedelta] = None,
    cancellation_type: Optional[Any] = None,
    activity_id: Optional[str] = None,
    summary: Optional[str] = None,
) -> Any:
    """Run a local activity from a callable-class reference."""
    return await start_local_activity_class(
        activity,
        arg,
        args=args,
        schedule_to_close_timeout=schedule_to_close_timeout,
        schedule_to_start_timeout=schedule_to_start_timeout,
        start_to_close_timeout=start_to_close_timeout,
        retry_policy=retry_policy,
        local_retry_threshold=local_retry_threshold,
        cancellation_type=cancellation_type,
        activity_id=activity_id,
        summary=summary,
    )


class unsafe:
    """Namespace for unsafe workflow calls, mirroring
    ``temporalio.workflow.unsafe``.
    """

    @staticmethod
    def is_replaying() -> bool:
        """Whether the workflow is replaying its checkpointed prefix."""
        return _runtime().runtime_is_replaying()

    @staticmethod
    def imports_passed_through() -> "AbstractContextManager[None]":
        """No-op context manager: there is no sandbox to pass imports
        through (ARCHITECTURE.md no-sandbox)."""
        return nullcontext()

    @staticmethod
    def is_read_only() -> bool:
        """Whether the workflow is currently in read-only mode — true while a
        query or update validator runs, where side effects are not allowed."""
        return _runtime().runtime_is_read_only()

    @staticmethod
    def in_sandbox() -> bool:
        """Always False: there is no workflow sandbox."""
        return False
