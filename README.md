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
`dbos.DBOSConfig`). Workflow and activity code runs unmodified.

Current pass rate: **12 of 19 runnable samples** (the rest are blocked on
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
| hello_cancellation | ⬜ Phase 3 (sync activities observe cancellation via heartbeat) |
| hello_async_activity_completion | ⬜ Phase 3 (async completion) |
| hello_continue_as_new | ⬜ Phase 3 (continue-as-new) |
| hello_cron | ⬜ Phase 3 (cron) |
| hello_search_attributes | ⬜ Phase 3 (search attributes) |
| hello_query | ⬜ Phase 4 (queries on closed workflows — deviation #2) |
| hello_activity_multiprocess | ⬜ multiprocess activity executors unsupported |
| hello_change_log_level | — never exits by design (also true on Temporal) |
| hello_mtls | — needs mTLS infrastructure |
| hello_patch | — manual multi-invocation walkthrough (Phase 4) |

## Known deviations from Temporal

This table is maintained as features land; see `DESIGN.md` §8 for details.

| # | Deviation |
|---|---|
| 1 | No Temporal server/UI/CLI; no non-Python clients. Operate via DBOS tooling. |
| 2 | Queries hit Postgres and (v1) require a RUNNING workflow. |
| 3 | No workflow sandbox: determinism violations surface at recovery/replay as `NondeterminismError`, not at development time. |
| 4 | Visibility query language: documented subset; custom search attributes stored but not indexed. |
| 5 | Default child-workflow IDs are derived from the parent, not random UUIDs. |
| 6 | `FAIL` id-conflict policy has a small TOCTOU window in v1. |
| 7 | Different latency/throughput profile: every effect is a Postgres write. Benchmarks will be published. |
| 8 | Payloads live in the DBOS system database; Temporal's 2MB/4MB payload caps are not enforced. |
| 9 | (Temporary, until the Phase 3 data-conversion pipeline) Payloads are serialized with pickle, not JSON: exact objects round-trip even without type hints, where temporalio's default converter would return plain dicts. Checkpoints written under pickle will not survive the switch. |

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
