# Conformance tests

Runs [temporalio/samples-python](https://github.com/temporalio/samples-python)
samples against temporal-dbos. The resulting pass-rate table (mirrored in the
top-level README) is the product's headline number (DESIGN.md §9).

How it works:

1. `samples.py` clones samples-python at a **pinned commit** into a cache
   under the system temp directory (network needed on first run).
2. The **mechanical migration step** rewrites the import root
   (`temporalio` → `temporal_dbos`) into a temp build directory.
3. `runner.py` executes one sample per subprocess with the
   **connection-setup adapter** installed — the documented migration delta:
   `Client.connect("host:port")` becomes a Client over a `DBOSClient`, and
   `Worker(client, ...)` becomes `Worker(DBOSConfig, ...)`. Everything else
   in the sample runs as written.
4. `test_hello_samples.py` parametrizes over the corpus with per-sample
   expectations; samples blocked on roadmap phases are `xfail` with the
   phase named in the reason.

Run with: `uv run pytest tests/conformance/` (needs Postgres, like all
integration tests).
