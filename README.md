# temporal-dbos

A drop-in replacement for the [Temporal Python SDK](https://github.com/temporalio/sdk-python)
(`temporalio`), backed by [DBOS Transact](https://github.com/dbos-inc/dbos-transact-py)
and Postgres instead of a Temporal server.

Code written against `temporalio` runs on `temporal_dbos` with an import-root swap: same
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
- **Partial support only** for Nexus, multi-namespace isolation, and advanced visibility
  (full query language, custom indexed search attributes).

## Conformance

The conformance suite (`tests/conformance/`) runs the
[temporalio/samples-python](https://github.com/temporalio/samples-python)
`hello/` corpus against temporal-dbos. Migration = the mechanical import
rewrite (`temporalio` → `temporal_dbos`) plus adapting connection setup
(`Client.connect` takes a `dbos.DBOSClient`; `Worker` takes a
`dbos.DBOSConfig`). Workflow and activity code runs unmodified. The
`message_passing/` corpus passes 5/5.

Current pass rate: **16 of 19 runnable samples** (the rest are blocked on
roadmap phases, noted below; 3 samples aren't runnable in any automated
harness).

| Sample | Status |
|---|---|
| hello_activity | ✅ |
| hello_activity_async | ✅ |
| hello_activity_choice | ✅ |
| hello_activity_heartbeat | ✅ |
| hello_activity_method | ✅ |
| hello_activity_retry | ✅ |
| hello_exception | ✅ |
| hello_local_activity | ✅ |
| hello_parallel_activity | ✅ |
| hello_signal | ✅ |
| hello_update | ✅ |
| hello_child_workflow | ✅ |
| hello_cancellation | ✅ (sync activity observes cancellation via heartbeat; cleanup runs in the unwind) |
| hello_async_activity_completion | ✅ (`raise_complete_async` + task-token completion) |
| hello_continue_as_new | ✅ (10 chained runs) |
| hello_cron | ✅ (cron chain fires and hops; the sample never exits, so the harness verifies through the database) |
| hello_search_attributes | ⚠️ storage implemented (memo + search attributes on DBOS workflow attributes; set at start, `upsert_*` from inside, read via `describe()`), but this sample is timing-flaky: it upserts 2s in and describes 3s later, a 1s margin our ~1s queue-dispatch latency races. Verified by unit + integration + recovery tests instead. |
| hello_query | ⬜ Phase 4 (queries on closed workflows — deviation #2) |
| hello_activity_multiprocess | ⬜ multiprocess activity executors unsupported |
| hello_change_log_level | — never exits by design (also true on Temporal) |
| hello_mtls | — needs mTLS infrastructure |
| hello_patch | — manual multi-invocation walkthrough (Phase 4) |

`message_passing/` (multi-file, worker + starter as separate processes):

| Sample | Status |
|---|---|
| introduction | ✅ (queries, updates + validators, start_update staging, signals, async update handlers running activities) |
| waiting_for_handlers | ✅ (`all_handlers_finished`) |
| waiting_for_handlers_and_compensation | ✅ (`workflow.wait`, compensation patterns) |
| update_with_start/lazy_initialization | ✅ (`WithStartWorkflowOperation`, `execute_update_with_start_workflow`) |
| safe_message_handlers | ✅ (continue-as-new + handler-heavy traffic) — the `message_passing/` corpus is complete |

`schedules/` (a long-running worker plus per-operation client scripts, run
unmodified):

| Sample | Status |
|---|---|
| start_schedule | ✅ (`create_schedule` with an interval `ScheduleSpec`) |
| describe_schedule | ✅ (`ScheduleHandle.describe` → `state.note`) |
| list_schedule | ✅ (`list_schedules` async iterator) |
| trigger_schedule | ✅ (`trigger` fires an action immediately) |
| update_schedule | ✅ (`update` callback; delete-then-recreate — deviation #16) |
| pause_schedule | ✅ (`pause` with note) |
| backfill_schedule | ✅ (`backfill` over a past window) |
| delete_schedule | ✅ (`delete`) |

`activity_worker/` (cross-queue / distributed activity dispatch, §6.1.2):

| Sample | Status |
|---|---|
| activity_worker | ✅ capability-substituted — the samples-python sample is a Go workflow calling a Python activity over a server (unrunnable here: no server, no Go worker). The conformance test proves the capability it demonstrates: an activities-only worker reachable from a workflow on a different task queue, with both workers as separate processes. SIGKILL recovery of either worker is covered (integration). Queued-path scope: see [DEVIATIONS.md](DEVIATIONS.md) D23. |

## Known deviations from Temporal

**[DEVIATIONS.md](DEVIATIONS.md) is the canonical, detailed record of
*fundamental* deviations** — those inherent to the serverless architecture
or deliberate design decisions. The table below is the summary; rows marked
temporary are phase-gaps tracked by the conformance suite, not fundamentals.

| # | Deviation |
|---|---|
| 1 | No Temporal server/UI/CLI; no non-Python clients. Operate via DBOS tooling. |
| 2 | Queries hit Postgres and (v1) require a RUNNING workflow. |
| 3 | No workflow sandbox: determinism violations surface at recovery/replay as `NondeterminismError`, not at development time. |
| 4 | Memo + search attributes are stored on DBOS workflow attributes (JSONB, GIN-indexed) — set at start, `upsert_*` from inside a workflow, read via `describe()`/`info()`. Search attributes are stored untyped (no cluster-side registration) and the visibility query language (`list_workflows(query=...)`/`count_workflows`) is not wired yet. See [DEVIATIONS.md](DEVIATIONS.md) D15. |
| 5 | Default child-workflow IDs are derived from the parent, not random UUIDs. |
| 6 | `FAIL` id-conflict policy has a small TOCTOU window in v1. |
| 7 | Different latency/throughput profile: every effect is a Postgres write. Benchmarks will be published. |
| 8 | Payloads live in the DBOS system database; Temporal's 2MB/4MB payload caps are not enforced, and history length is ungoverned (no ~50k-event cap pushing toward continue-as-new). |
| 9 | Signal-with-start / update-with-start are two steps, not one atomic request; transport-ish failures raise builtin `TimeoutError`/`RuntimeError` rather than Temporal's RPC error types; signal/cancel resends are not deduplicated (updates are); a narrow window during a continue-as-new transition can drop an id-addressed signal (the closing run forwards everything it can). |
| 10 | Workflow time derives from checkpointed participant clocks: monotonic, but client clock skew can step it forward. |
| 11 | `@workflow.query` handlers must be synchronous (temporalio deprecates async ones; we reject them). |
| 12 | Data conversion matches temporalio (default JSON with type-hint reconstruction — without a type hint a value returns as a plain dict, as in temporalio — plus custom `DataConverter`/`PayloadCodec` via `data_converter=`). Edges: protobuf payloads are unsupported (a corollary of #1, no non-Python clients); and a configured `PayloadCodec` is not applied to a few synchronously-serialized values — failure `details`/`last_heartbeat_details` and cron `last_completion` — which ride the payload converter without the async codec. |
| 13 | Cron workflows are run chains with per-run results: `result()` on a successful cron run returns that run's result (temporalio's would follow the chain forever); cancellation between runs — or during a retry attempt's backoff — takes effect at the next fire; 6/7-field cron expressions are accepted as an extension. |
| 14 | (Temporary) Workflow retry policies trigger on workflow *failures*. A run that exceeds its `run_timeout` is cancelled to TERMINATED and does **not** retry or continue a cron chain (Temporal raises TIMED_OUT and retries). `execution_timeout` (the whole-chain bound that also caps retry chains) is likewise unenforced — an unlimited retry policy retries without a time bound. Both land with the TIMED_OUT status-marker work. |
| 15 | Workflow-retry edges: `non_retryable_error_types` additionally matches envelope failure classes (a superset of Temporal's application-type-only matching), and unconsumed signals carry over to the next retry attempt instead of dying with the failed run. |
| 16 | Schedules (`create_schedule`/`ScheduleHandle`) back onto DBOS schedules: a `ScheduleSpec` compiles to one cron expression (intervals that divide a cron boundary are exact, others approximate; calendar `year`/interval `offset` dropped); the schedule's overlap policy honors SKIP/CANCEL_OTHER/TERMINATE_OTHER/ALLOW_ALL (CANCEL_OTHER doesn't wait for the cancelled run to finish; detection is grid-based and bounded) but rejects BUFFER_ONE/BUFFER_ALL, and a per-call `trigger`/`backfill` overlap override is not applied (only None/ALLOW_ALL accepted, others raise); `update` is delete-then-recreate and `pause`/`unpause` don't persist their `note`; schedule history (recent actions, action counts) and schedule `memo`/`search_attributes` are not tracked. See [DEVIATIONS.md](DEVIATIONS.md) D22. |

## Development

```bash
uv sync                 # install (includes dev dependencies)
uv run pytest tests/unit          # no database needed
uv run pytest                     # needs Postgres (see below)
uv run mypy             # strict type checking
uv run black . && uv run isort .  # format
```

Integration tests need a running Postgres server (CI provides one; locally, point tests
at your own). Configure with `TDB_TEST_SYSTEM_DATABASE_URL` or `PGHOST`/`PGPORT`/
`PGUSER`/`PGPASSWORD`. Tests drop and re-create their own databases on that server —
don't point them at a server whose databases you care about.
