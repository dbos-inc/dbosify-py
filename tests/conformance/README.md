# Conformance tests

Runs [temporalio/samples-python](https://github.com/temporalio/samples-python)
samples against DBOSify. The resulting pass-rate table (mirrored in the
top-level README) is the product's headline number (DESIGN.md §9).

How it works:

1. `samples.py` clones samples-python at a **pinned commit** into a cache
   under the system temp directory (network needed on first run).
2. The **mechanical migration step** rewrites the import root
   (`temporalio` → `dbosify`) into a temp build directory.
3. `runner.py` executes one sample per subprocess with the
   **connection-setup adapter** installed — the documented migration delta:
   `Client.connect("host:port")` becomes a Client over a `DBOSClient`, and
   `Worker(client, ...)` becomes `Worker(DBOSConfig, ...)`. Everything else
   in the sample runs as written.
4. `test_hello_samples.py` parametrizes over the corpus with per-sample
   expectations; samples blocked on roadmap phases are `xfail` with the
   phase named in the reason.

Three corpora:

- `test_hello_samples.py` — the `hello/` directory: single-file samples,
  one subprocess each (worker + starter in one process).
- `test_message_passing.py` — the `message_passing/` directory (the Phase 2
  exit gate): multi-file packages run as **two processes** (a long-running
  worker in module mode, then a starter driven to completion), with
  `rewrite_package` preserving their package-absolute imports.
- `test_schedules.py` — the `schedules/` directory (part of the Phase 3 exit
  gate): a long-running worker plus a sequence of per-operation client scripts
  (`start`/`describe`/`list`/`trigger`/`update`/`pause`/`backfill`/`delete`),
  run in dependency order; the flat directory is rewritten so the scripts'
  sibling imports resolve.

Run with: `uv run pytest tests/conformance/` (needs Postgres, like all
integration tests).
