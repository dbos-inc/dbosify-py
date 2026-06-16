"""Replayer integration tests (Phase 4): re-execute a recorded run's DBOS
checkpoints under the currently-registered code and detect non-determinism.

Each test records a run with a Worker up, then constructs a Replayer (which
re-registers the workflow type, so the fork runs *its* code) and replays the
fetched history. Divergence is simulated by module-level ``MODE`` flags that
change which activities the workflow runs — standing in for a code change
between the recorded run and the replay.
"""

from datetime import timedelta
from typing import AsyncIterator, Dict, List

import pytest
from dbos import DBOSClient

from temporal_dbos import activity, workflow
from temporal_dbos.client import Client, WorkflowFailureError, WorkflowHistory
from temporal_dbos.exceptions import ApplicationError
from temporal_dbos.worker import Replayer, Worker
from temporal_dbos.workflow import NondeterminismError
from tests.dbconfig import default_config, system_database_url

pytestmark = pytest.mark.usefixtures("tdb_env")

TASK_QUEUE = "replay-tq"
_OPTS: Dict[str, object] = {"start_to_close_timeout": timedelta(seconds=30)}

# Behaviour flags simulating a code change between record and replay.
MODE = {"reorder": False, "extra": False, "skip": False}
C_RUNS: List[int] = []


@pytest.fixture(autouse=True)
def _reset_mode() -> None:
    MODE.update(reorder=False, extra=False, skip=False)
    C_RUNS.clear()


@activity.defn
async def act_a(x: str) -> str:
    return f"a:{x}"


@activity.defn
async def act_b(x: str) -> str:
    return f"b:{x}"


@activity.defn
async def act_c(x: str) -> str:
    C_RUNS.append(1)
    return f"c:{x}"


@workflow.defn
class ReplayWf:
    @workflow.run
    async def run(self, x: str) -> str:
        if MODE["skip"]:
            await workflow.execute_activity(act_a, x, **_OPTS)  # type: ignore[arg-type]
            return "early"
        if MODE["reorder"]:
            await workflow.execute_activity(act_b, x, **_OPTS)  # type: ignore[arg-type]
            await workflow.execute_activity(act_a, x, **_OPTS)  # type: ignore[arg-type]
        else:
            await workflow.execute_activity(act_a, x, **_OPTS)  # type: ignore[arg-type]
            await workflow.execute_activity(act_b, x, **_OPTS)  # type: ignore[arg-type]
        if MODE["extra"]:
            await workflow.execute_activity(act_c, x, **_OPTS)  # type: ignore[arg-type]
        return "full"


@workflow.defn
class TimerSignalWf:
    def __init__(self) -> None:
        self._go = False

    @workflow.signal
    def go(self) -> None:
        self._go = True

    @workflow.run
    async def run(self, x: str) -> str:
        await workflow.wait_condition(lambda: self._go)
        await workflow.sleep(0.05)
        return await workflow.execute_activity(act_a, x, **_OPTS)  # type: ignore[arg-type]


@workflow.defn
class GreetingWf:
    def __init__(self) -> None:
        self._greeting = "<none>"

    @workflow.query
    def greeting(self) -> str:
        return self._greeting

    @workflow.run
    async def run(self, name: str) -> str:
        self._greeting = f"Hello, {name}!"
        await workflow.sleep(0.05)
        self._greeting = f"Goodbye, {name}!"
        return self._greeting


@workflow.defn
class ChildWf:
    @workflow.run
    async def run(self, x: str) -> str:
        return await workflow.execute_activity(act_a, x, **_OPTS)  # type: ignore[arg-type]


@workflow.defn
class ParentWf:
    @workflow.run
    async def run(self, x: str) -> str:
        child = await workflow.execute_child_workflow(ChildWf.run, x)
        return f"parent({child})"


@workflow.defn
class FailWf:
    @workflow.run
    async def run(self) -> str:
        await workflow.execute_activity(act_a, "x", **_OPTS)  # type: ignore[arg-type]
        raise ApplicationError("intentional boom", type="Boom")


def _worker(*workflows: type) -> Worker:
    return Worker(
        default_config(),
        task_queue=TASK_QUEUE,
        workflows=list(workflows) or [ReplayWf],
        activities=[act_a, act_b, act_c],
    )


async def _aiter(items: List[WorkflowHistory]) -> AsyncIterator[WorkflowHistory]:
    for item in items:
        yield item


async def test_replay_clean_no_failure() -> None:
    async with _worker():
        dbos_client = DBOSClient(system_database_url=system_database_url())
        try:
            client = await Client.connect(dbos_client)
            handle = await client.start_workflow(
                ReplayWf.run, "hi", id="rp-clean", task_queue=TASK_QUEUE
            )
            assert await handle.result() == "full"
            history = await handle.fetch_history()
            assert history.workflow_type == "ReplayWf"
            assert history.step_count > 0

            result = await Replayer(workflows=[ReplayWf]).replay_workflow(history)
            assert result.replay_failure is None
        finally:
            dbos_client.destroy()


async def test_replay_reordered_activity_diverges() -> None:
    async with _worker():
        dbos_client = DBOSClient(system_database_url=system_database_url())
        try:
            client = await Client.connect(dbos_client)
            handle = await client.start_workflow(
                ReplayWf.run, "hi", id="rp-reorder", task_queue=TASK_QUEUE
            )
            await handle.result()
            history = await handle.fetch_history()

            MODE["reorder"] = True  # the "code change": B before A
            replayer = Replayer(workflows=[ReplayWf])
            with pytest.raises(NondeterminismError):
                await replayer.replay_workflow(history)
            result = await replayer.replay_workflow(
                history, raise_on_replay_failure=False
            )
            assert isinstance(result.replay_failure, NondeterminismError)
        finally:
            dbos_client.destroy()


async def test_replay_added_trailing_activity_is_guarded() -> None:
    async with _worker():
        dbos_client = DBOSClient(system_database_url=system_database_url())
        try:
            client = await Client.connect(dbos_client)
            handle = await client.start_workflow(
                ReplayWf.run, "hi", id="rp-extra", task_queue=TASK_QUEUE
            )
            await handle.result()
            history = await handle.fetch_history()

            MODE["extra"] = True  # the "code change": an extra trailing activity
            result = await Replayer(workflows=[ReplayWf]).replay_workflow(
                history, raise_on_replay_failure=False
            )
            assert isinstance(result.replay_failure, NondeterminismError)
            # The guard fired before the new activity ran: no real side effect.
            assert C_RUNS == []
        finally:
            dbos_client.destroy()


async def test_replay_early_finish_diverges() -> None:
    async with _worker():
        dbos_client = DBOSClient(system_database_url=system_database_url())
        try:
            client = await Client.connect(dbos_client)
            handle = await client.start_workflow(
                ReplayWf.run, "hi", id="rp-skip", task_queue=TASK_QUEUE
            )
            await handle.result()
            history = await handle.fetch_history()

            MODE["skip"] = True  # the "code change": finishes before activity B
            result = await Replayer(workflows=[ReplayWf]).replay_workflow(
                history, raise_on_replay_failure=False
            )
            assert isinstance(result.replay_failure, NondeterminismError)
        finally:
            dbos_client.destroy()


async def test_recorded_failure_replays_as_pass() -> None:
    async with _worker(FailWf):
        dbos_client = DBOSClient(system_database_url=system_database_url())
        try:
            client = await Client.connect(dbos_client)
            handle = await client.start_workflow(
                FailWf.run, id="rp-fail", task_queue=TASK_QUEUE
            )
            with pytest.raises(WorkflowFailureError):
                await handle.result()
            history = await handle.fetch_history()

            # A workflow that faithfully re-fails is a *passing* replay: replay
            # verifies determinism, not success.
            result = await Replayer(workflows=[FailWf]).replay_workflow(
                history, raise_on_replay_failure=False
            )
            assert result.replay_failure is None
        finally:
            dbos_client.destroy()


async def test_replay_timer_and_signal_clean() -> None:
    async with _worker(TimerSignalWf):
        dbos_client = DBOSClient(system_database_url=system_database_url())
        try:
            client = await Client.connect(dbos_client)
            handle = await client.start_workflow(
                TimerSignalWf.run, "hi", id="rp-timer", task_queue=TASK_QUEUE
            )
            await handle.signal(TimerSignalWf.go)
            assert await handle.result() == "a:hi"
            history = await handle.fetch_history()

            result = await Replayer(workflows=[TimerSignalWf]).replay_workflow(history)
            assert result.replay_failure is None
        finally:
            dbos_client.destroy()


async def test_replay_with_child_workflow_clean() -> None:
    async with _worker(ParentWf, ChildWf):
        dbos_client = DBOSClient(system_database_url=system_database_url())
        try:
            client = await Client.connect(dbos_client)
            handle = await client.start_workflow(
                ParentWf.run, "hi", id="rp-parent", task_queue=TASK_QUEUE
            )
            assert await handle.result() == "parent(a:hi)"
            history = await handle.fetch_history()

            # Replay re-attaches to the recorded child (no twin spawned) and the
            # child-start command does not trip the horizon guard.
            result = await Replayer(
                workflows=[ParentWf, ChildWf]
            ).replay_workflow(history)
            assert result.replay_failure is None
        finally:
            dbos_client.destroy()


async def test_query_on_closed_workflow_rehydrates() -> None:
    async with _worker(GreetingWf):
        dbos_client = DBOSClient(system_database_url=system_database_url())
        try:
            client = await Client.connect(dbos_client)
            handle = await client.start_workflow(
                GreetingWf.run, "World", id="rp-query", task_queue=TASK_QUEUE
            )
            # Query while RUNNING goes through the live path.
            assert await handle.query(GreetingWf.greeting) in (
                "Hello, World!",
                "Goodbye, World!",
            )
            assert await handle.result() == "Goodbye, World!"

            # The workflow is now closed; the query rehydrates it by replay and
            # reads the reconstructed final state.
            assert await handle.query(GreetingWf.greeting) == "Goodbye, World!"

            # The scratch rehydrate fork was cleaned up (no extra runs linger).
            survivors = await dbos_client.list_workflows_async(
                workflow_id_prefix="", load_input=False
            )
            assert all(s.workflow_id == "rp-query" for s in survivors), [
                s.workflow_id for s in survivors
            ]
        finally:
            dbos_client.destroy()


async def test_replay_workflows_aggregates_failures_by_run_id() -> None:
    async with _worker():
        dbos_client = DBOSClient(system_database_url=system_database_url())
        try:
            client = await Client.connect(dbos_client)
            histories: List[WorkflowHistory] = []
            for i in range(2):
                handle = await client.start_workflow(
                    ReplayWf.run, "hi", id=f"rp-agg-{i}", task_queue=TASK_QUEUE
                )
                await handle.result()
                histories.append(await handle.fetch_history())

            replayer = Replayer(workflows=[ReplayWf])
            clean = await replayer.replay_workflows(
                _aiter(histories), raise_on_replay_failure=False
            )
            assert clean.replay_failures == {}

            MODE["reorder"] = True  # both now diverge
            diverged = await replayer.replay_workflows(
                _aiter(histories), raise_on_replay_failure=False
            )
            assert set(diverged.replay_failures) == {h.run_id for h in histories}
            assert all(
                isinstance(f, NondeterminismError)
                for f in diverged.replay_failures.values()
            )
        finally:
            dbos_client.destroy()
