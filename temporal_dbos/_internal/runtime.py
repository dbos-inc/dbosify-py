"""Shared DBOS lifecycle for Client and Worker (DESIGN.md §5).

temporalio usage patterns this must support: a starter process creates only
a ``Client``; a worker process creates a ``Client`` plus ``Worker``(s);
tests create both in one process. The corresponding modes:

  * **Client mode** — a lazy ``DBOSClient``: enqueue-by-name, send,
    get_event, list. No code registration, no recovery.
  * **Full mode** — constructing a ``Worker`` upgrades the process to a real
    ``DBOS`` instance (registrations + queues); ``worker.run()`` launches it
    (recovering pending workflows, mirroring Temporal worker restart
    semantics), refcounted across workers.

Both modes share one module-level ``_Runtime`` so a Client and Worker in the
same process agree on the database and schema.
"""

import logging
import os
import threading
from typing import Any, Dict, Optional

import sqlalchemy as sa
from dbos import DBOS, DBOSClient, DBOSConfig, Queue

logger = logging.getLogger("temporal_dbos.runtime")

# Worst-case latency for client-side get_event when a LISTEN/NOTIFY wakeup
# is missed (see docs/phase0.md); DBOSClient has no public knob yet.
CLIENT_POLL_ENV = "TEMPORAL_DBOS_CLIENT_POLL_SECONDS"
DEFAULT_CLIENT_POLL_SECONDS = 1.0

TARGET_ENV_FALLBACKS = ("DBOS_SYSTEM_DATABASE_URL", "DBOS_DATABASE_URL")


def resolve_target(target_host: str) -> str:
    """Interpret a ``Client.connect`` target (DESIGN §5).

    A Postgres URL is the system database URL. Anything host:port-shaped
    (including the Temporal default ``localhost:7233``, so unmodified
    samples work) falls back to the DBOS database-URL environment variables.
    """
    try:
        url = sa.make_url(target_host)
        drivername = url.drivername
    except Exception:
        drivername = ""
    if drivername.startswith("postgres"):
        return target_host
    for env in TARGET_ENV_FALLBACKS:
        from_env = os.environ.get(env)
        if from_env:
            return from_env
    raise ValueError(
        f"Cannot interpret target_host {target_host!r}: pass a Postgres URL "
        f"or set {TARGET_ENV_FALLBACKS[0]}"
    )


def namespace_to_schema(namespace: str) -> str:
    """Temporal namespace -> DBOS system schema. ``default`` maps to DBOS's
    default schema; multiple namespaces are cheap isolation via schemas.
    """
    if namespace == "default":
        return "dbos"
    if not namespace.replace("_", "").replace("-", "").isalnum():
        raise ValueError(f"Invalid namespace for temporal-dbos: {namespace!r}")
    return f"tdb_{namespace}".replace("-", "_")


class _Runtime:
    def __init__(self) -> None:
        self._lock = threading.Lock()
        self.system_database_url: Optional[str] = None
        self.schema: Optional[str] = None
        self.app_version: Optional[str] = None
        self._client: Optional[DBOSClient] = None
        self._dbos: Optional[DBOS] = None
        self._launch_refs = 0
        self._queues: Dict[str, Queue] = {}

    # -- configuration ------------------------------------------------------

    def configure(self, system_database_url: str, schema: str) -> None:
        with self._lock:
            if self.system_database_url is None:
                self.system_database_url = system_database_url
                self.schema = schema
                return
            if (self.system_database_url, self.schema) != (
                system_database_url,
                schema,
            ):
                raise RuntimeError(
                    "temporal-dbos supports one database/namespace per process: "
                    f"already configured for {self.system_database_url!r} "
                    f"(schema {self.schema!r})"
                )

    def _require_configured(self) -> str:
        if self.system_database_url is None:
            raise RuntimeError("Not connected: create a Client first")
        return self.system_database_url

    # -- client mode ---------------------------------------------------------

    def client(self) -> DBOSClient:
        with self._lock:
            if self._client is None:
                self._client = DBOSClient(
                    system_database_url=self._require_configured(),
                    dbos_system_schema=self.schema,
                )
                poll = float(
                    os.environ.get(CLIENT_POLL_ENV, str(DEFAULT_CLIENT_POLL_SECONDS))
                )
                # Private until DBOS exposes an option (docs/phase0.md).
                self._client._sys_db._notification_fallback_polling_interval = poll
            return self._client

    # -- full (worker) mode ---------------------------------------------------

    def ensure_full_runtime(self, *, app_version: Optional[str]) -> None:
        with self._lock:
            if self._dbos is not None:
                if app_version and app_version != self.app_version:
                    logger.debug(
                        "Ignoring build_id %r: DBOS already constructed with %r",
                        app_version,
                        self.app_version,
                    )
                return
            config: DBOSConfig = {
                "name": "temporal_dbos",
                "system_database_url": self._require_configured(),
                "dbos_system_schema": self.schema,
                "run_admin_server": False,
            }
            if app_version:
                config["application_version"] = app_version
            self.app_version = app_version
            self._dbos = DBOS(config=config)

    def register_queue(self, name: str, *, worker_concurrency: Optional[int]) -> Queue:
        with self._lock:
            queue = self._queues.get(name)
            if queue is None:
                queue = Queue(name, worker_concurrency=worker_concurrency)
                self._queues[name] = queue
            elif worker_concurrency is not None:
                logger.debug(
                    "Queue %r already registered; ignoring worker_concurrency=%r",
                    name,
                    worker_concurrency,
                )
            return queue

    def launch(self) -> None:
        """Refcounted DBOS.launch: the first worker's run() launches (which
        also recovers this executor's pending workflows)."""
        with self._lock:
            assert self._dbos is not None, "ensure_full_runtime first"
            self._launch_refs += 1
            if self._launch_refs == 1:
                DBOS.launch()

    def release(self, *, workflow_completion_timeout_sec: int = 0) -> None:
        with self._lock:
            self._launch_refs -= 1
            if self._launch_refs > 0:
                return
            DBOS.destroy(
                destroy_registry=False,
                workflow_completion_timeout_sec=workflow_completion_timeout_sec,
            )
            self._dbos = None
            self._queues.clear()

    # -- tests ----------------------------------------------------------------

    def _reset_for_tests(self) -> None:
        from . import dispatcher

        with self._lock:
            if self._client is not None:
                self._client.destroy()
                self._client = None
            self._dbos = None
            self._launch_refs = 0
            self._queues.clear()
            self.system_database_url = None
            self.schema = None
            self.app_version = None
        DBOS.destroy(destroy_registry=True)
        dispatcher._reset_for_tests()


_runtime = _Runtime()


def get_runtime() -> _Runtime:
    return _runtime
