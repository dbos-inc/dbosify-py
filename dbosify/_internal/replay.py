"""Workflow replay: re-execute a recorded run's DBOS checkpoints under the
currently-registered code to detect non-determinism, mirroring
``temporalio.worker.Replayer``.

Mechanism (DESIGN.md §9, "Replayer over DBOS step checkpoints / fork_workflow"):
re-execution from checkpoints is exactly the path crash-recovery already takes,
and DBOS itself raises ``DBOSUnexpectedStepError`` when a re-run claims a
different step at a recorded ``function_id``. So a replay is a *fork* of the
source run at one step past its last recorded step (``replay_horizon + 1``),
which copies every recorded checkpoint into a fresh scratch run and re-executes
the whole workflow function: each step replays from its copy (no real activity
runs), and any divergence surfaces as a failure on the scratch run's result.
After verifying, the scratch fork is deleted.

Two divergence kinds DBOS does *not* catch on its own are handled by the
interpreter while it replays a scratch fork:

* **extends beyond history** — current code launches a *new* durable operation
  past the recorded horizon (would execute a real activity in the fork);
* **finishes early** — current code completes having claimed fewer steps than
  were recorded.

The interpreter recognizes "I am a replay, and which kind" from the scratch
run's *own* workflow id: ``start_replay_fork`` names the fork deterministically
with a reserved suffix (``--v`` verify, ``--q`` rehydrate; see ``ids``) via
``SetWorkflowID``, and the interpreter reads ``ids.replay_scratch_mode`` on its
own id. Because the signal lives in the id rather than in the forker's process
memory, *whichever* worker dequeues the fork runs it correctly — the fork is
created with no pinned application version, so a worker in a different process
than the forker (e.g. a pure client querying a closed run) handles it. The
horizon the interpreter checks against is the one it derives from its own copied
steps, so nothing about the replay has to be passed out of band.

The same fork machinery (``start_replay_fork`` with ``mode``) backs
**query-on-closed-workflow rehydrate** (DEVIATIONS replay): the client forks a
closed run in ``mode="rehydrate"``, the forked run replays to its final state
and then *keeps serving* one query against the reconstructed instance (it is not
deleted-after-verify but stops on a client signal or the
``REHYDRATE_SERVE_SECONDS`` deadline), and the client then discards it. The
``REHYDRATE_*`` constants below tune that serve/settle/poll timing.

There is at most one scratch per (run, mode), so the id is a fixed suffix. A
scratch left behind by a crash is reclaimed simply by re-forking the same id
(``start_replay_fork`` deletes any stale scratch first); the trade-off is that
two concurrent replays/rehydrates of the *same* run and mode contend for the one
id rather than each spinning up their own.
"""

import logging
from contextlib import asynccontextmanager
from dataclasses import dataclass, field
from typing import (
    TYPE_CHECKING,
    Any,
    AsyncIterator,
    Dict,
    List,
    Mapping,
    Optional,
    Sequence,
)

from . import ids

if TYPE_CHECKING:
    from ..client import WorkflowHistory
    from ..converter import DataConverter

logger = logging.getLogger("dbosify.replay")

# Failure-envelope ``type`` marker the dispatcher stamps on a divergence so the
# engine can tell "the replay diverged" from "the workflow faithfully re-failed".
NONDETERMINISM_MARKER = "__dbosify_nondeterminism__"

# How long a rehydrated (query-on-closed) scratch run keeps serving queries
# before completing on its own, if the client never sends a stop signal.
REHYDRATE_SERVE_SECONDS = 30.0

# How long the querying client waits for a rehydrate scratch run to settle
# (after sending the stop signal) before force-cancelling it and deleting it.
REHYDRATE_SETTLE_SECONDS = 10.0

# Inbox-recv timeout a rehydrate scratch run uses while serving queries, so its
# serve deadline stays effective even if the client never sends a stop signal.
REHYDRATE_POLL_SECONDS = 1.0


# ---------------------------------------------------------------------------
# Results (mirror temporalio.worker)
# ---------------------------------------------------------------------------


@dataclass(frozen=True)
class WorkflowReplayResult:
    """Result of replaying a single workflow history."""

    history: "WorkflowHistory"
    replay_failure: Optional[Exception]


@dataclass(frozen=True)
class WorkflowReplayResults:
    """Results of replaying multiple workflow histories."""

    replay_failures: Dict[str, Exception] = field(default_factory=dict)
    """Replay failures keyed by the run id that failed."""


# ---------------------------------------------------------------------------
# Engine
# ---------------------------------------------------------------------------


async def start_replay_fork(
    forker: Any,
    run_id: str,
    steps: Sequence[Mapping[str, Any]],
    *,
    mode: str = "verify",
) -> Any:
    """Fork ``run_id`` one step past its last recorded checkpoint — copying
    *every* recorded step — into a deterministically-named scratch run. Returns
    the fork handle.

    The scratch id is ``ids.replay_scratch_id(run_id, mode)`` (a reserved ``--v``
    / ``--q`` suffix), forced with ``SetWorkflowID``. Naming it — rather than
    tracking it in this process's memory — is what lets a worker in a *different*
    process recognize and correctly run the replay: it reads the mode off its own
    id. The fork is created with no pinned application version (NULL), so the
    current worker for the type (anywhere) dequeues and runs it, verifying
    today's code rather than the recorded run's version.

    Fork bound: the copy is ``function_id < start_step`` and ids are 1-based and
    contiguous, so ``horizon + 1`` includes the final step. ``forker`` is
    anything exposing ``fork_workflow_async`` + ``delete_workflow_async`` (the
    ``DBOS`` runtime or a ``DBOSClient``).

    There is one scratch per (run, mode), so a stale scratch from a crashed prior
    replay/rehydrate would block the fork; reclaim it first (best-effort delete).
    """
    from dbos import SetWorkflowID

    horizon = max((s["function_id"] for s in steps), default=0)
    scratch_id = ids.replay_scratch_id(run_id, mode)
    try:
        # delete_children=False: the scratch parents no real children.
        await forker.delete_workflow_async(scratch_id, delete_children=False)
    except Exception:  # noqa: BLE001 — no stale scratch to reclaim is the norm
        pass
    with SetWorkflowID(scratch_id):
        return await forker.fork_workflow_async(run_id, horizon + 1)


async def replay_one(history: "WorkflowHistory") -> Optional[Exception]:
    """Fork-and-verify a single history under the currently-registered code.

    Returns the replay failure (a ``NondeterminismError`` for divergence, or a
    ``ValueError`` for a history that cannot be replayed) or None if the run
    replayed faithfully. Requires a launched DBOS runtime (see :class:`Replayer`).
    """
    from dbos import DBOS
    from dbos import error as dbos_error

    from ..workflow import NondeterminismError
    from .payloads import (
        SerializedContinueAsNew,
        SerializedWorkflowCancellation,
        SerializedWorkflowFailure,
    )
    from .status import WorkflowExecutionStatus

    # States that cannot be faithfully reconstructed by replay (DEVIATIONS
    # replay): partial-history (TERMINATED/TIMED_OUT) or CONTINUED_AS_NEW.
    if history.status in (
        WorkflowExecutionStatus.TERMINATED,
        WorkflowExecutionStatus.TIMED_OUT,
        WorkflowExecutionStatus.CONTINUED_AS_NEW,
    ):
        return ValueError(
            f"cannot replay a {history.status.name} workflow: it cannot be "
            "faithfully reconstructed from its checkpoints (DEVIATIONS replay)"
        )

    handle = await start_replay_fork(
        DBOS, history.run_id, history.recorded_steps, mode="verify"
    )
    scratch_id = handle.get_workflow_id()
    replay_failure: Optional[Exception] = None
    try:
        try:
            # Poll briskly: the scratch fork only replays recorded checkpoints,
            # so it finishes in well under the 1s default poll interval.
            await handle.get_result(polling_interval_sec=0.1)
        except SerializedWorkflowCancellation:
            # Faithful cancellation -> PASS. Caught before SerializedWorkflowFailure
            # because it is a subclass that the broader clause would shadow.
            pass
        except SerializedWorkflowFailure as failure:
            if failure.envelope.get("type") == NONDETERMINISM_MARKER:
                replay_failure = NondeterminismError(
                    str(failure.envelope.get("message", "nondeterministic replay"))
                )
            # else: a genuine recorded failure replayed faithfully -> PASS.
        except SerializedContinueAsNew:
            pass  # faithful continue-as-new -> PASS
        except (
            dbos_error.DBOSUnexpectedStepError
        ) as err:  # defensive: surfaced directly
            replay_failure = NondeterminismError(str(err))
        except NondeterminismError as err:  # defensive: surfaced directly
            replay_failure = err
    finally:
        try:
            # delete_children stays False: a replayed fork starts no real children
            # (it parents nothing) and we must never touch the source run's subtree.
            await DBOS.delete_workflow_async(scratch_id, delete_children=False)
        except Exception:  # noqa: BLE001 — cleanup is best-effort
            logger.warning("replay: failed to delete scratch fork %s", scratch_id)
    return replay_failure


# ---------------------------------------------------------------------------
# Replayer
# ---------------------------------------------------------------------------


class Replayer:
    """Replays workflow histories to detect non-determinism, mirroring
    ``temporalio.worker.Replayer``.

    Unlike temporalio's server-backed replayer, this re-executes a run's DBOS
    step checkpoints, so it operates within **this process's launched DBOS
    runtime**: construct a :class:`~dbosify.worker.Worker` for the
    workflow types under test (which launches DBOS, registers their
    dispatchers, and installs the data converter / interceptors / failure
    types), then replay histories fetched via ``WorkflowHandle.fetch_history()``.
    The fork re-enters the Worker's registered code, so the Worker *is* the code
    under test.

    The Replayer reuses that Worker's process-global configuration and does not
    mutate it: ``data_converter``, ``interceptors``, and
    ``workflow_failure_exception_types`` are accepted for API parity but the
    running Worker's values are authoritative (overriding them here would
    clobber the live Worker, since one Worker owns the process; DEVIATIONS replay).
    Parameters with no dbosify analog (``namespace``, ``build_id``,
    ``identity``, ``workflow_runner``/``unsandboxed_workflow_runner``,
    ``debug_mode``, ``runtime``, ``plugins``, ``workflow_task_executor``, ...)
    are accepted and ignored with a debug log.
    """

    def __init__(
        self,
        *,
        workflows: Sequence[type],
        data_converter: Optional["DataConverter"] = None,
        interceptors: Sequence[Any] = (),
        workflow_failure_exception_types: Sequence[type] = (),
        **unsupported: Any,
    ) -> None:
        if not workflows:
            raise ValueError("At least one workflow must be specified")
        for key in unsupported:
            logger.debug("Replayer: ignoring unsupported option %r", key)
        if data_converter is not None:
            logger.debug(
                "Replayer: data_converter is inherited from the running Worker; "
                "the passed value is ignored to avoid clobbering it"
            )

        from . import registry

        # Validate that each type is registered by a running Worker (the fork
        # re-enters its dispatcher); we do not mutate that process-global state.
        self._workflow_names: List[str] = []
        for cls in workflows:
            defn = registry.workflow_definition_of(cls)
            if defn.name not in registry._dbos_workflows:
                raise RuntimeError(
                    f"Workflow type {defn.name!r} is not registered; construct a "
                    "Worker for the types under test before replaying"
                )
            self._workflow_names.append(defn.name)

    async def replay_workflow(
        self,
        history: "WorkflowHistory",
        *,
        raise_on_replay_failure: bool = True,
    ) -> WorkflowReplayResult:
        """Replay a single history. Raises the replay failure if one occurs and
        ``raise_on_replay_failure`` is set; otherwise returns it on the result."""
        failure = await replay_one(history)
        if failure is not None and raise_on_replay_failure:
            raise failure
        return WorkflowReplayResult(history=history, replay_failure=failure)

    async def replay_workflows(
        self,
        histories: AsyncIterator["WorkflowHistory"],
        *,
        raise_on_replay_failure: bool = True,
    ) -> WorkflowReplayResults:
        """Replay multiple histories, aggregating failures keyed by run id."""
        results = WorkflowReplayResults()
        first_failure: Optional[Exception] = None
        async for history in histories:
            failure = await replay_one(history)
            if failure is not None:
                results.replay_failures[history.run_id] = failure
                if first_failure is None:
                    first_failure = failure
        if first_failure is not None and raise_on_replay_failure:
            raise first_failure
        return results

    @asynccontextmanager
    async def workflow_replay_iterator(
        self, histories: AsyncIterator["WorkflowHistory"]
    ) -> AsyncIterator[AsyncIterator[WorkflowReplayResult]]:
        """Context manager yielding an async iterator of per-history results
        (never raising on a replay failure — inspect ``replay_failure``)."""

        async def _iter() -> AsyncIterator[WorkflowReplayResult]:
            async for history in histories:
                failure = await replay_one(history)
                yield WorkflowReplayResult(history=history, replay_failure=failure)

        yield _iter()
