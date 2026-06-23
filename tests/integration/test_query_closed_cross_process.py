"""Cross-process query-on-closed: a pure client (no co-located worker) queries a
CLOSED workflow whose only worker runs in a *separate* OS process.

This is the regression test for the divergence that motivated naming replay
scratch forks by id: previously the rehydrate "guard" lived in the forker's
process memory, so a worker in another process would not recognize the fork and
the query failed. With the guard's role moved into the scratch run's own id, any
worker for the type serves it — matching Temporal, where a closed-workflow query
is answered by any worker on the task queue, not one co-located with the client.
"""

from datetime import timedelta
from pathlib import Path

import pytest

from dbosify.client import WorkflowExecutionStatus
from tests.dbconfig import connect_client
from tests.harness import PythonProcess, retry_until_success_async
from tests.integration.rehydrate_xproc_worker import TASK_QUEUE, GreetingWf

pytestmark = pytest.mark.usefixtures("dbosify_env")

_WORKER = Path(__file__).parent / "rehydrate_xproc_worker.py"


def _worker_process() -> PythonProcess:
    return PythonProcess(_WORKER, env={"PYTHONPATH": str(Path(__file__).parents[2])})


async def test_pure_client_queries_closed_workflow_cross_process() -> None:
    worker = _worker_process()
    worker.start()
    try:
        worker.wait_for_line("READY", timeout=60)

        # Pure client: no Worker here, so only the subprocess can run the fork.
        client = await connect_client()
        try:
            handle = await client.start_workflow(
                GreetingWf.run, "World", id="xproc-1", task_queue=TASK_QUEUE
            )
            assert await handle.result() == "Goodbye, World!"

            async def _is_completed() -> None:
                desc = await handle.describe()
                assert desc.status == WorkflowExecutionStatus.COMPLETED

            await retry_until_success_async(_is_completed)

            # Closed-workflow query: forked here, served by the subprocess worker.
            assert (
                await handle.query(
                    GreetingWf.greeting, rpc_timeout=timedelta(seconds=30)
                )
                == "Goodbye, World!"
            )

            # A second query also works: the first query's scratch was torn down.
            assert (
                await handle.query(
                    GreetingWf.greeting, rpc_timeout=timedelta(seconds=30)
                )
                == "Goodbye, World!"
            )

            # Scratch forks are gone and never surfaced; only the real run remains.
            survivors = [w async for w in client.list_workflows()]
            assert [w.id for w in survivors] == ["xproc-1"], [w.id for w in survivors]
        finally:
            await client.close()
    finally:
        worker.terminate_and_wait()
