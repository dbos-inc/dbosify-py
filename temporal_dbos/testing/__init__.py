"""Test framework module, mirroring ``temporalio.testing``.

:py:class:`ActivityEnvironment` runs activity code in memory, no database.
:py:class:`WorkflowEnvironment.start_local` provisions an isolated,
throwaway database on an externally provided Postgres server (there is no
dev server to download — Postgres *is* the server). Time-skipping is Phase 4
(see DESIGN §6.10).
"""

import asyncio
import os
import secrets
from datetime import datetime, timedelta, timezone
from typing import Any, Callable, Coroutine, Optional, TypeVar, Union, overload
from urllib.parse import quote

import sqlalchemy
from dbos import DBOSClient, DBOSConfig

from .. import activity as _activity
from ..client import Client

__all__ = ["ActivityEnvironment", "WorkflowEnvironment"]

_R = TypeVar("_R")

_default_info = _activity.Info(
    activity_id="test",
    activity_type="unknown",
    attempt=1,
    task_queue="test",
    workflow_id="test",
    workflow_run_id="test-run",
    workflow_type="test",
    is_local=False,
)


class ActivityEnvironment:
    """Activity environment for testing activity code directly: runs the
    function with the activity context (``activity.info()``, ``heartbeat``,
    cancellation observation) set, entirely in memory.

    Attributes:
        info: The info handed to activities; replace to customize.
        on_heartbeat: Called with the details of each ``activity.heartbeat``.
    """

    def __init__(self, client: Optional[Any] = None) -> None:
        # `client`, when given, is returned by ``activity.client()`` inside the
        # run activity (temporalio parity).
        self.info: _activity.Info = _default_info
        self.on_heartbeat: Callable[..., None] = lambda *args: None
        self._context = _activity._Context(
            info=self.info,
            on_heartbeat=lambda *details: self.on_heartbeat(*details),
            client=client,
        )

    def cancel(self) -> None:
        """Mark the environment's activity as cancelled: ``is_cancelled()``
        becomes true and ``wait_for_cancelled*`` unblocks.
        """
        self._context.cancelled.set()

    def worker_shutdown(self) -> None:
        """Mark the environment's worker as shut down, mirroring
        ``temporalio.testing.ActivityEnvironment.worker_shutdown``:
        ``is_worker_shutdown()`` becomes true and ``wait_for_worker_shutdown*``
        unblocks.
        """
        self._context.worker_shutdown_event.set()

    @overload
    def run(
        self,
        fn: Callable[..., Coroutine[Any, Any, _R]],
        *args: Any,
        **kwargs: Any,
    ) -> Coroutine[Any, Any, _R]: ...

    @overload
    def run(self, fn: Callable[..., _R], *args: Any, **kwargs: Any) -> _R: ...

    def run(self, fn: Callable[..., Any], *args: Any, **kwargs: Any) -> Any:
        """Run the given activity callable in this environment. Returns the
        result, or a coroutine to await if the activity is async.
        """
        self._context.info = self.info

        if asyncio.iscoroutinefunction(fn):

            async def run_async() -> Any:
                token = _activity._current_context.set(self._context)
                try:
                    return await fn(*args, **kwargs)
                finally:
                    _activity._current_context.reset(token)

            return run_async()

        token = _activity._current_context.set(self._context)
        try:
            return fn(*args, **kwargs)
        finally:
            _activity._current_context.reset(token)


def _server_url_from_env() -> str:
    """The Postgres to host throwaway environment databases on:
    ``DBOS_SYSTEM_DATABASE_URL`` if set, else ``PG*`` variables with the
    same defaults DBOS uses.
    """
    url = os.environ.get("DBOS_SYSTEM_DATABASE_URL")
    if url is not None:
        return url
    host = os.environ.get("PGHOST", "localhost")
    port = os.environ.get("PGPORT", "5432")
    user = os.environ.get("PGUSER", "postgres")
    password = quote(os.environ.get("PGPASSWORD", "dbos"), safe="")
    return f"postgresql+psycopg://{user}:{password}@{host}:{port}/postgres"


class WorkflowEnvironment:
    """Workflow environment for testing whole workflows, mirroring
    ``temporalio.testing.WorkflowEnvironment``.

    :py:meth:`start_local` provisions a uniquely named, throwaway database
    on an externally provided Postgres server (it never launches one) and
    tears it down on :py:meth:`shutdown`. Build the environment's Worker
    from :py:attr:`dbos_config`; drive it with :py:attr:`client`.
    """

    def __init__(self, client: Client) -> None:
        self._client = client
        self._dbos_client: Optional[DBOSClient] = None
        self._dbos_config: Optional[DBOSConfig] = None
        self._maintenance_url: Optional[str] = None
        self._database: Optional[str] = None

    @classmethod
    def from_client(cls, client: Client) -> "WorkflowEnvironment":
        """An environment over an existing client; :py:meth:`shutdown` does
        not tear anything down."""
        return cls(client)

    @classmethod
    async def start_local(
        cls, *, system_database_url: Optional[str] = None
    ) -> "WorkflowEnvironment":
        """Start a local test environment: a fresh ``temporal_dbos_env_*``
        database on the given Postgres server (or the one resolved from
        ``DBOS_SYSTEM_DATABASE_URL``/``PG*``; the URL's own database name is
        ignored — databases are created via the server's ``postgres`` db).
        """
        base = sqlalchemy.make_url(system_database_url or _server_url_from_env())
        database = f"temporal_dbos_env_{secrets.token_hex(4)}"
        maintenance_url = base.set(database="postgres").render_as_string(
            hide_password=False
        )
        env_url = base.set(database=database).render_as_string(hide_password=False)

        def _create() -> None:
            engine = sqlalchemy.create_engine(
                maintenance_url, isolation_level="AUTOCOMMIT"
            )
            try:
                with engine.connect() as conn:
                    conn.execute(sqlalchemy.text(f'CREATE DATABASE "{database}"'))
            finally:
                engine.dispose()

        await asyncio.to_thread(_create)
        dbos_client = DBOSClient(system_database_url=env_url)
        env = cls(Client(dbos_client))
        env._dbos_client = dbos_client
        env._maintenance_url = maintenance_url
        env._database = database
        env._dbos_config = {
            "name": "temporal_dbos_env",
            "system_database_url": env_url,
            "run_admin_server": False,
            # Tests want fast signal/event delivery.
            "notification_listener_polling_interval_sec": 0.05,
        }
        return env

    @classmethod
    async def start_time_skipping(cls) -> "WorkflowEnvironment":
        raise NotImplementedError(
            "time-skipping test environments are not supported yet (Phase 4)"
        )

    @property
    def client(self) -> Client:
        """Client for this environment."""
        return self._client

    @property
    def dbos_config(self) -> DBOSConfig:
        """The DBOSConfig to construct this environment's Worker from
        (temporal-dbos extension; Workers take a config, not a client)."""
        if self._dbos_config is None:
            raise RuntimeError(
                "dbos_config is only available on environments created by "
                "start_local"
            )
        return self._dbos_config

    @property
    def supports_time_skipping(self) -> bool:
        """Whether this environment supports time-skipping (it does not)."""
        return False

    async def sleep(self, duration: Union[timedelta, float]) -> None:
        """Sleep in this environment (real time; no skipping)."""
        await asyncio.sleep(
            duration.total_seconds() if isinstance(duration, timedelta) else duration
        )

    def get_current_time(self) -> datetime:
        """Get the current time known to this environment."""
        return datetime.now(timezone.utc)

    async def shutdown(self) -> None:
        """Tear the environment down: destroy its client and drop its
        database (no-op for :py:meth:`from_client` environments)."""
        if self._dbos_client is not None:
            await asyncio.to_thread(self._dbos_client.destroy)
            self._dbos_client = None
        if self._database is not None:
            maintenance_url, database = self._maintenance_url, self._database
            assert maintenance_url is not None
            self._database = None

            def _drop() -> None:
                engine = sqlalchemy.create_engine(
                    maintenance_url, isolation_level="AUTOCOMMIT"
                )
                try:
                    with engine.connect() as conn:
                        conn.execute(
                            sqlalchemy.text(
                                f'DROP DATABASE IF EXISTS "{database}" WITH (FORCE)'
                            )
                        )
                finally:
                    engine.dispose()

            await asyncio.to_thread(_drop)

    async def __aenter__(self) -> "WorkflowEnvironment":
        return self

    async def __aexit__(self, *exc_info: Any) -> None:
        await self.shutdown()
