# DBOSify

A drop-in replacement for the [Temporal Python SDK](https://github.com/temporalio/sdk-python) backed by a Postgres database (through [DBOS Transact](https://github.com/dbos-inc/dbos-transact-py)) instead of a Temporal server.

To use this library, import `dbosify` instead of `temporalio` and connect your workers and clients to a Postgres database instead of a Temporal server.
The library uses Postgres to orchestrate your durable workflows and messaging, providing the same reliability guarantees with no infrastructure requirements.
All you need is Postgres.

<p align="center">
  <img src="docs/architecture.png" alt="DBOSify architecture: a DBOSify Client and DBOSify Workers coordinate through Postgres, which handles workflow orchestration" width="720">
</p>

## Usage

To install:

```shell
pip install dbosify
```

This is a drop-in replacement: simply import `dbosify` instead of `temporalio` and connect your clients and workers to a Postgres database instead of a Temporal server.
Further documentation [here](https://docs.dbos.dev/explanations/migrating-from-temporal).

```python
import asyncio
from datetime import timedelta

from dbosify import activity, workflow
from dbosify.client import Client
from dbosify.worker import Worker

DB_URL = "postgresql://postgres:dbos@localhost:5432/dbosify"


@activity.defn
async def compose_greeting(name: str) -> str:
    return f"Hello, {name}!"


@workflow.defn
class GreetingWorkflow:
    @workflow.run
    async def run(self, name: str) -> str:
        return await workflow.execute_activity(
            compose_greeting, name, start_to_close_timeout=timedelta(seconds=10)
        )


async def main() -> None:
    worker = Worker(
        DB_URL,
        task_queue="greetings",
        workflows=[GreetingWorkflow],
        activities=[compose_greeting],
    )
    async with worker:
        async with await Client.connect(DB_URL) as client:
            result = await client.execute_workflow(
                GreetingWorkflow.run, "World", id="greeting-1", task_queue="greetings"
            )
            print(result)  # Hello, World!


if __name__ == "__main__":
    asyncio.run(main())
```

## How It Works

DBOSify runs each workflow as a durable DBOS workflow backed by Postgres.
A deterministic interpreter runs workflows (their main coroutines and their signal, update, and query handlers) on a virtual event loop that only advances when an event arrives.
Using DBOS steps and [workflow communication primitives](https://docs.dbos.dev/python/tutorials/workflow-communication), all nondeterministic actions are checkpointed in Postgres before being observed by the workflow.

- **Activities and timers** become DBOS steps and durable sleeps, each checkpointed on completion.
- **Signals, updates, and cancellations** are durable messages delivered through Postgres.
- **Recovery** re-runs the workflow on a new worker: the interpreter replays the same sequence of operations against the recorded checkpoints, so execution resumes where it left off and completes exactly once.
- **Namespaces** each map to their own Postgres schema; a `Client` wraps a DBOS client and a `Worker` wraps the DBOS runtime.

## How It's Tested

As DBOSify is a drop-in replacement for Temporal, we test both correctness and conformance with Temporal.
This repository incorporates following testing strategies:

- Direct ports of all relevant Temporal Python unit and integration tests
- Direct ports of relevant Temporal Python sample applications, verifying DBOSify is a drop-in replacement
- New unit and integration tests, with an emphasis on kill-and-recover tests verifying deterministic failure recovery
- Signature parity tests mechanically asserting the public APIs of these libraries are identical (with documented exceptions)

## What This Is Not

- **Not a Temporal server replacement.** There is no gRPC wire compatibility. Temporal SDKs in other languages cannot connect. This replaces the Temporal server and Python SDK altogether for Python-only applications.
- **No Temporal Web UI, `temporal` CLI, or tctl.** You operate workflows with DBOS's workflow-management APIs and DBOS Conductor instead.

See [this documentation](./docs/ARCHITECTURE.md) for information on architectural differences and feature compatibility.
