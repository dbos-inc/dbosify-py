"""Subprocess worker for the DBOS foundational-semantics tests.

Run as: python dbos_semantics_worker.py <mode> <workflow_id> [effects_path]

Modes:
  chaos-start   launch DBOS, start the chaos workflow, await its result
  chaos-resume  launch DBOS (recovery picks up the pending workflow), await result
  recv-start    launch DBOS, start the recv-ordering workflow, await result
  recv-resume   launch DBOS (recovery), await result

The chaos workflow reproduces the exact concurrency pattern the dbosify
interpreter is built on: many concurrently-executing async DBOS steps raced
with DBOS.asyncio_wait(FIRST_COMPLETED) in rounds, with new steps launched as
earlier ones complete. It validates that function_id assignment and the
asyncio_wait winner checkpoints replay deterministically across a SIGKILL.
"""

import asyncio
import json
import sys
from typing import Any, Callable, Coroutine

from dbos import DBOS, SetWorkflowID

from tests.dbconfig import default_config

INITIAL_STEPS = [("a", 0.8), ("b", 0.1), ("c", 0.4), ("d", 0.05)]
CHAIN_SLEEP = 0.3


def _make_step(name: str) -> Callable[[str], Coroutine[Any, Any, str]]:
    durations = dict(INITIAL_STEPS)

    @DBOS.step(name=f"chaos_{name}")
    async def chaos_step(effects_path: str) -> str:
        # Effects-file append marks an *execution* (for at-most-once checks);
        # the prints let the test SIGKILL at a known checkpoint state.
        print(f"STEP_START {name}", flush=True)
        with open(effects_path, "a") as f:
            f.write(name + "\n")
        await asyncio.sleep(durations.get(name, CHAIN_SLEEP))
        print(f"STEP_DONE {name}", flush=True)
        return f"value-{name}"

    return chaos_step


CHAOS_STEPS = {
    name: _make_step(name)
    for name in [n for n, _ in INITIAL_STEPS] + [n + "2" for n, _ in INITIAL_STEPS]
}


@DBOS.workflow()
async def chaos_workflow(effects_path: str) -> dict[str, Any]:
    # Launch four steps whose completion order (d, b, c, a) differs from
    # launch order, then race them in FIRST_COMPLETED rounds, chaining a
    # follow-up step after each initial step completes — the interpreter's
    # event-loop pattern.
    pending: list[asyncio.Task[str]] = []
    names: dict[asyncio.Task[str], str] = {}

    def launch(name: str) -> None:
        # The @DBOS.step wrapper assigns its function_id synchronously at
        # call time, so launch order fixes checkpoint order.
        task = asyncio.ensure_future(CHAOS_STEPS[name](effects_path))
        pending.append(task)
        names[task] = name

    for name, _ in INITIAL_STEPS:
        launch(name)

    order: list[str] = []
    results: dict[str, str] = {}
    while pending:
        # Let newly created tasks run their sync prefix (function_id
        # snapshot) in creation order before the next checkpointed wait.
        await asyncio.sleep(0)
        done, _ = await DBOS.asyncio_wait(
            list(pending), return_when=asyncio.FIRST_COMPLETED
        )
        for task in list(pending):  # iterate in deterministic list order
            if task in done:
                name = names[task]
                order.append(name)
                results[name] = task.result()
                pending.remove(task)
                if not name.endswith("2"):
                    launch(name + "2")
    return {"order": order, "results": results}


@DBOS.workflow()
async def recv_workflow() -> list[str]:
    received: list[str] = []
    for i in range(3):
        msg = await DBOS.recv_async("inbox", timeout_seconds=60)
        received.append(str(msg))
        print(f"RECEIVED {i + 1}", flush=True)
    return received


def main() -> None:
    mode, workflow_id = sys.argv[1], sys.argv[2]
    effects_path = sys.argv[3] if len(sys.argv) > 3 else ""

    DBOS(config=default_config())
    DBOS.launch()  # on -resume modes, recovery re-executes the pending workflow
    print("LAUNCHED", flush=True)

    handle: Any
    if mode == "chaos-start":
        with SetWorkflowID(workflow_id):
            handle = DBOS.start_workflow(chaos_workflow, effects_path)
    elif mode == "recv-start":
        with SetWorkflowID(workflow_id):
            handle = DBOS.start_workflow(recv_workflow)
    elif mode in ("chaos-resume", "recv-resume"):
        handle = DBOS.retrieve_workflow(workflow_id)
    else:
        raise ValueError(f"unknown mode {mode}")

    # Only after this marker does the workflow row exist; DBOS.send to a
    # nonexistent workflow fails on a foreign-key constraint.
    print("STARTED", flush=True)
    result = handle.get_result()
    print("RESULT " + json.dumps(result), flush=True)
    DBOS.destroy()


if __name__ == "__main__":
    main()
