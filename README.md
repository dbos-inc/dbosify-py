# DBOSify

A drop-in replacement for the [Temporal Python SDK](https://github.com/temporalio/sdk-python)
(`temporalio`), backed by [DBOS Transact](https://github.com/dbos-inc/dbos-transact-py)
and Postgres instead of a Temporal server.

Code written against `temporalio` runs on `dbosify` with an import-root swap: same
decorators, same call signatures, same exception types, same durability guarantees —
workflows survive process crashes and resume correctly — with no Temporal server to
operate. All you need is Postgres.

**Status: pre-alpha.** Under active development; see `DESIGN.md` for the architecture and
roadmap.

## What this is not

- **Not a Temporal server replacement.** There is no gRPC wire compatibility; Temporal
  SDKs in other languages cannot connect. This replaces server + Python SDK together,
  for Python-only applications.
- **No Temporal Web UI, `temporal` CLI, or tctl.** You operate workflows with DBOS's
  workflow-management APIs and DBOS Conductor instead.
- **Partial support only** for Nexus and advanced visibility (full query language, custom
  indexed search attributes). Namespaces are supported as per-namespace Postgres schemas,
  one namespace per process (see `docs/DEVIATIONS.md` no-server).

## Usage

Define workflows and activities exactly as in `temporalio`; only the import root
changes. A `Worker` owns the process's DBOS runtime and takes either a Postgres URL
or a `dbos.DBOSConfig`:

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

## Known deviations from Temporal

**[DEVIATIONS.md](docs/DEVIATIONS.md) is the canonical, detailed record of
*fundamental* deviations** — those inherent to the serverless architecture
or deliberate design decisions. The table below is the summary; rows marked
temporary are phase-gaps tracked by the conformance suite, not fundamentals.

| # | Deviation |
|---|---|
| 1 | No Temporal server/UI/CLI; no non-Python clients. Operate via DBOS tooling. |
| 2 | Queries hit Postgres. A RUNNING workflow is queried directly; a *closed* workflow is queried by rehydrate-by-replay (the `Replayer` machinery re-executes its checkpoints to reconstruct final state, answers, and discards the scratch run) — which requires a worker for that type in the querying process. See [DEVIATIONS.md](docs/DEVIATIONS.md) replay. |
| 3 | No workflow sandbox: determinism violations surface at recovery/replay as `NondeterminismError`, not at development time. |
| 4 | Memo + search attributes are stored on DBOS workflow attributes (JSONB, GIN-indexed) — set at start, `upsert_*` from inside a workflow, read via `describe()`/`info()`. Search attributes are stored untyped (no cluster-side registration). `list_workflows(query=...)`/`count_workflows` support a documented subset of the visibility query language (a flat `AND` of `WorkflowType`/`WorkflowId`/`ExecutionStatus`/time-range/search-attribute predicates, plus an optional `GROUP BY ExecutionStatus`/`WorkflowType` for `count`; no `OR`/grouping/`ORDER BY`). `count` runs entirely on DBOS's server-side `COUNT`/`GROUP BY` aggregate and rejects (rather than scans) queries it can't express — search-attribute/exact-id/error-status filters and `GROUP BY ExecutionStatus`. See [DEVIATIONS.md](docs/DEVIATIONS.md) memo-search-attributes. |
| 5 | Default child-workflow IDs are derived from the parent, not random UUIDs. |
| 6 | `FAIL` id-conflict policy has a small TOCTOU window in v1. |
| 7 | Different latency/throughput profile: every effect is a Postgres write. Benchmarks will be published. |
| 8 | Payloads live in the DBOS system database; Temporal's 2MB/4MB payload caps are not enforced, and history length is ungoverned (no ~50k-event cap pushing toward continue-as-new). |
| 9 | Signal-with-start / update-with-start are two steps, not one atomic request; transport-ish failures raise builtin `TimeoutError`/`RuntimeError` rather than Temporal's RPC error types; signal/cancel resends are not deduplicated (updates are); a narrow window during a continue-as-new transition can drop an id-addressed signal (the closing run forwards everything it can). |
| 10 | Workflow time derives from checkpointed participant clocks: monotonic, but client clock skew can step it forward. |
| 11 | `@workflow.query` handlers must be synchronous (temporalio deprecates async ones; we reject them). |
| 12 | Data conversion matches temporalio (default JSON with type-hint reconstruction — without a type hint a value returns as a plain dict, as in temporalio — plus custom `DataConverter`/`PayloadCodec` via `data_converter=`). Edges: protobuf payloads are unsupported (no non-Python clients); and a configured `PayloadCodec` is not applied to a few synchronously-serialized values — failure `details`/`last_heartbeat_details` and cron `last_completion` — which ride the payload converter without the async codec. |
| 13 | Cron workflows are run chains with per-run results: `result()` on a successful cron run returns that run's result (temporalio's would follow the chain forever); cancellation between runs — or during a retry attempt's backoff — takes effect at the next fire; 6/7-field cron expressions are accepted as an extension. |
| 14 | (Temporary) Workflow retry policies trigger on workflow *failures*. A run that exceeds its `run_timeout` is cancelled to TERMINATED and does **not** retry or continue a cron chain (Temporal raises TIMED_OUT and retries). `execution_timeout` (the whole-chain bound that also caps retry chains) is likewise unenforced — an unlimited retry policy retries without a time bound. Both land with the TIMED_OUT status-marker work. |
| 15 | Workflow-retry edges: `non_retryable_error_types` additionally matches envelope failure classes (a superset of Temporal's application-type-only matching), and unconsumed signals carry over to the next retry attempt instead of dying with the failed run. |
| 16 | Schedules (`create_schedule`/`ScheduleHandle`) back onto DBOS schedules: a `ScheduleSpec` compiles to one cron expression (intervals that divide a cron boundary are exact, others approximate; calendar `year`/interval `offset` dropped); the schedule's overlap policy honors SKIP/CANCEL_OTHER/TERMINATE_OTHER/ALLOW_ALL (CANCEL_OTHER doesn't wait for the cancelled run to finish; detection is grid-based and bounded) but rejects BUFFER_ONE/BUFFER_ALL, and a per-call `trigger`/`backfill` overlap override is not applied (only None/ALLOW_ALL accepted, others raise); `update` is delete-then-recreate and `pause`/`unpause` don't persist their `note`; schedule history (recent actions, action counts) and schedule `memo`/`search_attributes` are not tracked. See [DEVIATIONS.md](docs/DEVIATIONS.md) schedules. |
| 17 | Interceptors cover client (`Client(interceptors=)`), activity, and workflow inbound/outbound (`Worker(interceptors=)`, via `workflow_interceptor_class`). Header-based context propagation works end-to-end (client→workflow→activity/child/signal, re-injected across continue-as-new); header values are `Payload`s, encoded/decoded with `workflow.payload_converter()`/`activity.payload_converter()`. `handle_query`/`handle_update_validator` are synchronous. Nexus interception is unsupported; worker interceptors come only from `Worker(interceptors=)` (the worker has no client to harvest them from). |
| 18 | Dynamic **signal/query/update handlers** (`dynamic=True`, with `description=` metadata) and dynamic **activities** (`@activity.defn(dynamic=True)`) are supported, including `workflow`/`activity.payload_converter()` for converting their `RawValue` args. Dynamic **workflows** (`@workflow.defn(dynamic=True)`) are **not** supported and raise `NotImplementedError`: a catch-all workflow has no per-type `wf:{type}` DBOS registration (one-workflow-per-type listing). See [DEVIATIONS.md](docs/DEVIATIONS.md) dynamic-handlers. |
| 19 | `workflow.patched()` / `deprecate_patch()` are supported (durable checkpoint markers; a False verdict claims no position so pre-patch runs replay the old path). Use `patched()` for in-code branching across deploys. |
| 20 | Worker **deployment versioning** maps onto DBOS versioning: a build ID *is* the DBOS `application_version` (set via `Worker(build_id=)`/`Worker(deployment_config=)`, readable via `workflow.info().get_current_deployment_version()`/`get_current_build_id()`). Because DBOS scopes recovery and queue dequeue to `application_version`, **PINNED is enforced** — a workflow is recovered/continued only on workers of its build ID and never auto-migrates. What's unsupported is **AUTO_UPGRADE** (moving a running workflow to a newer version) and cluster ramping/routing; those requests (`VersioningBehavior.AUTO_UPGRADE`, `AutoUpgradeVersioningOverride`, `versioning_intent`) degrade to pinned, and `is_target_worker_deployment_version_changed()` is always `False`. See [DEVIATIONS.md](docs/DEVIATIONS.md) worker-versioning. |
| 21 | `workflow.set_current_details()`/`get_current_details()` back free-form UI/CLI details as in-memory workflow state, reconstructed on replay; settable on the deterministic loop (run/handlers), not surfaced to `describe()` in v1. Activity context helpers — `activity.is_worker_shutdown()`/`wait_for_worker_shutdown[_sync]()`, `shield_thread_cancel_exception()` (no-op, cooperative cancel), and `activity.client()` (lazily built from the Worker's `DBOSConfig`) — are supported. See [DEVIATIONS.md](docs/DEVIATIONS.md) current-details. |
| 22 | **Metrics are not implemented in v1.** `workflow.metric_meter()`/`activity.metric_meter()`, the `common.MetricMeter`/`MetricCounter`/`MetricHistogram`/`MetricGauge` tree, and the entire `temporalio.runtime` telemetry module (`Runtime`, `TelemetryConfig`, `PrometheusConfig`, `OpenTelemetryConfig`, …) have no `dbosify` equivalent — calling `metric_meter()` raises `AttributeError` and `import dbosify.runtime` fails. Instrument with `prometheus_client`/OpenTelemetry directly for now. See [DEVIATIONS.md](docs/DEVIATIONS.md) no-metrics. |

## Development

```bash
uv sync                 # install (includes dev dependencies)
uv run pytest tests/unit          # no database needed
uv run pytest                     # needs Postgres (see below)
uv run mypy             # strict type checking
uv run black . && uv run isort .  # format
```

Integration tests need a running Postgres server (CI provides one; locally, point tests
at your own). Configure with `DBOSIFY_TEST_SYSTEM_DATABASE_URL` or `PGHOST`/`PGPORT`/
`PGUSER`/`PGPASSWORD`. Tests drop and re-create their own databases on that server —
don't point them at a server whose databases you care about.
