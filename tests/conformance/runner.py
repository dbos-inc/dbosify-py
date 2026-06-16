"""Subprocess entry point that runs one rewritten sample.

Usage: python runner.py <rewritten_sample.py>
Env:   TDB_CONFORMANCE_SYSTEM_DATABASE_URL — the (disposable) database.

Installs the connection-setup adapter — the documented migration delta
between temporalio and temporal-dbos — then executes the sample's
``__main__`` unchanged:

  * ``temporal_dbos.envconfig.ClientConfig`` (the samples load connection
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
    return os.environ["TDB_CONFORMANCE_SYSTEM_DATABASE_URL"]


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

    from temporal_dbos.client import Client
    from temporal_dbos.worker import Worker

    # -- temporal_dbos.envconfig stand-in ------------------------------------
    envconfig = types.ModuleType("temporal_dbos.envconfig")

    class ClientConfig:
        @staticmethod
        def load_client_connect_config(*args: Any, **kwargs: Any) -> dict[str, Any]:
            return {}

    envconfig.ClientConfig = ClientConfig  # type: ignore[attr-defined]
    sys.modules["temporal_dbos.envconfig"] = envconfig

    # -- Client.connect(target_host=str) -------------------------------------
    original_connect = Client.connect.__func__  # type: ignore[attr-defined]

    async def patched_connect(cls: Any, *args: Any, **kwargs: Any) -> Any:
        target = args[0] if args else kwargs.get("target_host")
        if isinstance(target, str) or target is None:
            return cls(DBOSClient(system_database_url=url))
        return await original_connect(cls, *args, **kwargs)

    Client.connect = classmethod(patched_connect)  # type: ignore[assignment, method-assign]

    # -- Worker(client, ...) --------------------------------------------------
    original_worker_init = Worker.__init__

    def patched_worker_init(self: Any, first: Any, *args: Any, **kwargs: Any) -> None:
        if isinstance(first, Client):
            config: DBOSConfig = {
                "name": "tdb_conformance",
                "system_database_url": url,
                "run_admin_server": False,
                "notification_listener_polling_interval_sec": 0.01,
            }
            original_worker_init(self, config, *args, **kwargs)
            # Near-immediate queue dispatch for low-latency conformance. The
            # default 1s queue poll (and the queue worker's first-poll wait
            # before any dequeue) adds ~1s before an enqueued workflow starts,
            # which fixed-time samples (hello_search_attributes upserts 2s in
            # and describes 3s later) race against. Declaring the queue with a
            # short interval *before* launch makes the worker thread start at
            # that interval. Mirrors the lowered notification poll above.
            from dbos import Queue

            Queue(self._task_queue, polling_interval_sec=0.05)
        else:
            original_worker_init(self, first, *args, **kwargs)

    Worker.__init__ = patched_worker_init  # type: ignore[assignment, method-assign]


def main() -> None:
    install_shim()
    target = sys.argv[1]
    # Samples that argparse must see a clean argv (defaults only), not the
    # runner's target argument.
    sys.argv = [target]
    if target.endswith(".py"):
        runpy.run_path(target, run_name="__main__")
    else:
        # Module mode for package samples (python -m equivalent); the
        # package root comes in on PYTHONPATH from the test.
        runpy.run_module(target, run_name="__main__", alter_sys=True)


if __name__ == "__main__":
    main()
