# DBOSify

A drop-in replacement for the [Temporal Python SDK](https://github.com/temporalio/sdk-python) backed by a Postgres database (through [DBOS Transact](https://github.com/dbos-inc/dbos-transact-py)) instead of a Temporal server.

To use this library, import `dbosify` instead of `temporalio` and connect your workers and clients to a Postgres database instead of a Temporal server.
The library uses Postgres to orchestrate your durable workflows and messaging, providing the same reliablity guarantees with no infrastructure requirements.
All you need is Postgres.

<p align="center">
  <img src="docs/architecture.png" alt="DBOSify architecture: a DBOSify Client and DBOSify Workers coordinate through Postgres, which handles workflow orchestration" width="720">
  <br>
  <sub><em>DBOSify clients and workers coordinate through Postgres — no Temporal server required.</em></sub>
</p>

## Usage

This is a drop-in replacement: simply import `dbosify` instead of `temporalio` and connect your clients and workers to a Postgres database instead of a Temporal server.
Further documentation [here](https://docs.dbos.dev/explanations/migrating-from-temporal).

```python
import asyncio
from datetime import timedelta

from dbosify import activity, workflow
from dbosify.client import Client
from dbosify.worker import Worker

DB_URL = "postgresql+psycopg://postgres:dbos@localhost:5432/dbosify"


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

## What this is not

- **Not a Temporal server replacement.** There is no gRPC wire compatibility; Temporal
  SDKs in other languages cannot connect. This replaces server + Python SDK together,
  for Python-only applications.
- **No Temporal Web UI, `temporal` CLI, or tctl.** You operate workflows with DBOS's
  workflow-management APIs and DBOS Conductor instead.

See [this documentation](./docs/ARCHITECTURE.md) for information on architectural differences and feature compatibility.
