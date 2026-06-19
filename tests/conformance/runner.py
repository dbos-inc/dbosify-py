"""Subprocess entry point that runs one rewritten sample.

Usage: python runner.py <rewritten_sample.py>
Env:   DBOSIFY_CONFORMANCE_SYSTEM_DATABASE_URL — the (disposable) database.

Installs the connection-setup adapter — the documented migration delta
between temporalio and dbosify — then executes the sample's
``__main__`` unchanged:

  * ``dbosify.envconfig.ClientConfig`` (the samples load connection
    config through it) -> returns an empty config.
  * ``Client.connect(target_host="localhost:7233", ...)`` -> a Client over a
    DBOSClient for the conformance database.
  * ``Worker(client, ...)`` -> ``Worker(DBOSConfig, ...)``.

Everything else — workflow/activity code, start/signal/query/update calls,
result handling — runs exactly as written in the sample.
"""

import os
import runpy
import sys
import types
from typing import Any

import sqlalchemy as sa


def _database_url() -> str:
    return os.environ["DBOSIFY_CONFORMANCE_SYSTEM_DATABASE_URL"]


def _ensure_database_exists(url_str: str) -> None:
    # The samples construct a Client before any Worker launches, and
    # DBOSClient connects at construction — so the database must pre-exist.
    url = sa.make_url(url_str)
    maintenance = sa.create_engine(
        url.set(drivername="postgresql+psycopg", database="postgres"),
        connect_args={"connect_timeout": 30},
    )
    try:
        with maintenance.connect() as conn:
            conn.execution_options(isolation_level="AUTOCOMMIT")
            exists = conn.execute(
                sa.text("SELECT 1 FROM pg_database WHERE datname = :name"),
                {"name": url.database},
            ).scalar()
            if not exists:
                conn.execute(sa.text(f'CREATE DATABASE "{url.database}"'))
    finally:
        maintenance.dispose()


def install_shim() -> None:
    url = _database_url()
    _ensure_database_exists(url)

    from dbos import DBOSClient, DBOSConfig

    from dbosify._internal.namespaces import DEFAULT_NAMESPACE, namespace_schema
    from dbosify.client import Client
    from dbosify.worker import Worker

    # The default namespace maps to its own DBOS schema (DEVIATIONS no-server); the
    # client must use it, and the Worker derives the same from namespace="default".
    schema = namespace_schema(DEFAULT_NAMESPACE)

    # -- dbosify.envconfig stand-in ------------------------------------
    envconfig = types.ModuleType("dbosify.envconfig")

    class ClientConfig:
        @staticmethod
        def load_client_connect_config(*args: Any, **kwargs: Any) -> dict[str, Any]:
            return {}

    envconfig.ClientConfig = ClientConfig  # type: ignore[attr-defined]
    sys.modules["dbosify.envconfig"] = envconfig

    # -- Client.connect(target_host=str) -------------------------------------
    original_connect = Client.connect.__func__  # type: ignore[attr-defined]

    async def patched_connect(cls: Any, *args: Any, **kwargs: Any) -> Any:
        target = args[0] if args else kwargs.get("target_host")
        if isinstance(target, str) or target is None:
            # Forward the connection options that are real migration surface
            # (not gRPC plumbing): a custom DataConverter / PayloadCodec and
            # interceptors. Our Client.__init__ accepts both.
            forwarded = {
                k: kwargs[k] for k in ("data_converter", "interceptors") if k in kwargs
            }
            client = cls(
                DBOSClient(system_database_url=url, dbos_system_schema=schema),
                **forwarded,
            )
            # Stash interceptors so the adapted ``Worker(client, ...)`` can
            # harvest them: temporalio Workers inherit the client's
            # interceptors, but ours take them via ``Worker(interceptors=)``
            # Bridging it here lets the sample run unmodified.
            client._conformance_interceptors = list(forwarded.get("interceptors", []))
            return client
        return await original_connect(cls, *args, **kwargs)

    Client.connect = classmethod(patched_connect)  # type: ignore[assignment, method-assign]

    # -- dbosify.api.common.v1 stand-in --------------------------------
    # Some samples import ``temporalio.api.common.v1.Payload`` purely for type
    # annotations (e.g. context_propagation's interceptor, under
    # ``from __future__ import annotations`` so it is never evaluated). The
    # protobuf API is a non-goal (DEVIATIONS no-server) and our Payload is a
    # lightweight dict; register the module chain so annotation-only imports
    # resolve. Samples that actually *construct* a protobuf Payload
    # (custom_converter, encryption) still fail at runtime — correctly, since
    # they depend on protobuf semantics we do not provide.
    import dbosify as _dbosify_pkg
    from dbosify import converter as _dbosify_converter

    for modname in (
        "dbosify.api",
        "dbosify.api.common",
        "dbosify.api.common.v1",
    ):
        sys.modules.setdefault(modname, types.ModuleType(modname))
    sys.modules["dbosify.api.common.v1"].Payload = _dbosify_converter.Payload  # type: ignore[attr-defined]
    sys.modules["dbosify.api.common"].v1 = sys.modules["dbosify.api.common.v1"]  # type: ignore[attr-defined]
    sys.modules["dbosify.api"].common = sys.modules["dbosify.api.common"]  # type: ignore[attr-defined]
    _dbosify_pkg.api = sys.modules["dbosify.api"]  # type: ignore[attr-defined]

    # -- Worker(client, ...) --------------------------------------------------
    original_worker_init = Worker.__init__

    def patched_worker_init(self: Any, first: Any, *args: Any, **kwargs: Any) -> None:
        if isinstance(first, Client):
            # Harvest the client's interceptors (temporalio Workers inherit
            # them; ours take them explicitly).
            harvested = getattr(first, "_conformance_interceptors", None)
            if harvested and "interceptors" not in kwargs:
                kwargs["interceptors"] = harvested
            config: DBOSConfig = {
                "name": "dbosify_conformance",
                "system_database_url": url,
                "run_admin_server": False,
                "notification_listener_polling_interval_sec": 0.01,
            }
            original_worker_init(self, config, *args, **kwargs)
            # Opt-in, ONE sample (hello_search_attributes): it upserts 2s into
            # the workflow and describes 3s later — a 1s margin that DBOS's ~1s
            # queue-dispatch latency (the queue worker's first-poll wait) races.
            # Declaring the queue with a short poll BEFORE launch makes the
            # worker thread dispatch near-immediately. Gated by env so it
            # touches ONLY that sample: applied broadly, this in-memory
            # declaration races the Worker's own post-launch database-backed
            # registration and breaks the cancellation/terminate-reuse dequeue
            # path (it regressed message_passing once).
            if os.environ.get("DBOSIFY_CONFORMANCE_FAST_QUEUE"):
                from dbos import Queue

                Queue(self._task_queue, polling_interval_sec=0.05)
        else:
            original_worker_init(self, first, *args, **kwargs)

    Worker.__init__ = patched_worker_init  # type: ignore[assignment, method-assign]


def main() -> None:
    install_shim()
    target = sys.argv[1]
    # Present the sample a clean argv: its own target as argv[0], then any
    # extra args the test passed after the target (e.g. dsl's YAML file) — but
    # not the runner's own target argument.
    sys.argv = [target, *sys.argv[2:]]
    if target.endswith(".py"):
        runpy.run_path(target, run_name="__main__")
    else:
        # Module mode for package samples (python -m equivalent); the
        # package root comes in on PYTHONPATH from the test.
        runpy.run_module(target, run_name="__main__", alter_sys=True)


if __name__ == "__main__":
    main()
