"""Unit tests for the replay surface that need no Postgres: WorkflowHistory
math, the NondeterminismError type, the scratch-id naming scheme that lets a
replay be recognized cross-process, and Replayer constructor validation."""

from typing import Any, Dict, List

import pytest

from dbosify import workflow
from dbosify._internal import ids, replay
from dbosify.client import WorkflowHistory
from dbosify.exceptions import TemporalError
from dbosify.worker import (
    Replayer,
    WorkflowReplayResult,
    WorkflowReplayResults,
)


def _steps(*function_ids: int) -> List[Dict[str, Any]]:
    return [{"function_id": fid, "function_name": f"s{fid}"} for fid in function_ids]


def test_history_horizon_and_count() -> None:
    h = WorkflowHistory(
        workflow_id="w", run_id="w", workflow_type="T", recorded_steps=_steps(1, 2, 5)
    )
    assert h.replay_horizon == 5
    assert h.step_count == 3


def test_history_empty_horizon_is_zero() -> None:
    h = WorkflowHistory(workflow_id="w", run_id="w", workflow_type="T")
    assert h.replay_horizon == 0
    assert h.step_count == 0


def test_nondeterminism_error_is_temporal_error() -> None:
    err = workflow.NondeterminismError("boom")
    assert isinstance(err, TemporalError)
    assert err.message == "boom"
    assert str(err) == "boom"


def test_scratch_id_round_trips_mode() -> None:
    # The mode is encoded in the id so any worker that runs the fork — including
    # one in a different process than the forker — recognizes the replay.
    for run_id in ("w", "w--r3", "w--r2_5"):
        verify = ids.replay_scratch_id(run_id, "verify")
        rehydrate = ids.replay_scratch_id(run_id, "rehydrate")
        assert verify != rehydrate
        assert ids.replay_scratch_mode(verify) == "verify"
        assert ids.replay_scratch_mode(rehydrate) == "rehydrate"
        assert ids.is_replay_scratch(verify) and ids.is_replay_scratch(rehydrate)


def test_real_run_ids_are_not_replay_scratch() -> None:
    # A real run, child, activity, or chained run must never be mistaken for a
    # scratch fork (which would hide it from visibility and skip its close logic).
    for real in ("w", "w--r1", "w--r2_5", "w--a3", "my-order-123"):
        assert ids.replay_scratch_mode(real) is None
        assert not ids.is_replay_scratch(real)


def test_scratch_separators_are_reserved_in_user_ids() -> None:
    for bad in ("order--v", "order--q", "a--qb"):
        with pytest.raises(ValueError, match="reserved separator"):
            ids.validate_workflow_id(bad)


def test_replayer_requires_at_least_one_workflow() -> None:
    with pytest.raises(ValueError):
        Replayer(workflows=[])


@workflow.defn
class _UnregisteredReplayProbe:
    @workflow.run
    async def run(self) -> None:
        return None


def test_replayer_requires_registered_workflow() -> None:
    # The Replayer reuses a running Worker's registered dispatchers; an
    # unregistered type cannot be replayed, and construction must say so loudly.
    with pytest.raises(RuntimeError, match="not registered"):
        Replayer(workflows=[_UnregisteredReplayProbe])


def test_replay_result_types() -> None:
    h = WorkflowHistory(workflow_id="w", run_id="r", workflow_type="T")
    result = WorkflowReplayResult(history=h, replay_failure=None)
    assert result.history is h and result.replay_failure is None
    results = WorkflowReplayResults()
    assert results.replay_failures == {}


async def test_replay_one_rejects_non_replayable_states() -> None:
    # The status gate returns before any fork, so this needs no DBOS runtime.
    from dbosify.client import WorkflowExecutionStatus

    for status in (
        WorkflowExecutionStatus.TERMINATED,
        WorkflowExecutionStatus.TIMED_OUT,
        WorkflowExecutionStatus.CONTINUED_AS_NEW,
    ):
        history = WorkflowHistory(
            workflow_id="w", run_id="w--r0", workflow_type="T", status=status
        )
        failure = await replay.replay_one(history)
        assert isinstance(failure, ValueError)
        assert status.name in str(failure)


async def test_fetch_history_events_not_supported() -> None:
    from dbosify.client import WorkflowHandle

    handle = WorkflowHandle(None, "wf-id")  # type: ignore[arg-type]
    with pytest.raises(NotImplementedError):
        await handle.fetch_history_events()
