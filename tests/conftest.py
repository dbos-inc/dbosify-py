"""Shared test fixtures.

Integration tests need a running Postgres server, provisioned externally (CI
service container or a local installation). Tests never launch a server; they
drop and re-create their own databases on the provided one.

Connection configuration, in priority order:
1. ``TDB_TEST_SYSTEM_DATABASE_URL`` — full SQLAlchemy URL; the database it
   names is dropped/created by tests, so never point it at a database you
   care about.
2. ``PGHOST``/``PGPORT``/``PGUSER``/``PGPASSWORD`` (defaults: localhost,
   5432, postgres, dbos), with the test database name below.
"""

import os
from typing import Any, Generator
from urllib.parse import quote

import pytest
import sqlalchemy as sa
from dbos import DBOS, DBOSClient, DBOSConfig

TEST_SYSTEM_DB_NAME = "temporal_dbos_test_dbos_sys"


def system_database_url() -> str:
    url = os.environ.get("TDB_TEST_SYSTEM_DATABASE_URL")
    if url is not None:
        return url
    host = os.environ.get("PGHOST", "localhost")
    port = os.environ.get("PGPORT", "5432")
    user = os.environ.get("PGUSER", "postgres")
    password = quote(os.environ.get("PGPASSWORD", "dbos"), safe="")
    return f"postgresql+psycopg://{user}:{password}@{host}:{port}/{TEST_SYSTEM_DB_NAME}"


def default_config() -> DBOSConfig:
    return {
        "name": "temporal_dbos_test",
        "system_database_url": system_database_url(),
        "run_admin_server": False,
        # Speeds up recv/event delivery in tests.
        "notification_listener_polling_interval_sec": 0.01,
    }


@pytest.fixture()
def config() -> DBOSConfig:
    return default_config()


@pytest.fixture(scope="session")
def maintenance_engine() -> Generator[sa.Engine, Any, None]:
    """Engine connected to the server's `postgres` database, for DDL on test DBs."""
    engine = sa.create_engine(
        sa.make_url(system_database_url()).set(
            drivername="postgresql+psycopg", database="postgres"
        ),
        connect_args={"connect_timeout": 30},
    )
    yield engine
    engine.dispose()


@pytest.fixture()
def cleanup_test_databases(maintenance_engine: sa.Engine) -> None:
    """Drop the test system database so each test starts from a blank slate.

    DBOS re-creates its system database on launch.
    """
    db_name = sa.make_url(system_database_url()).database
    assert db_name is not None
    with maintenance_engine.connect() as connection:
        connection.execution_options(isolation_level="AUTOCOMMIT")
        connection.execute(sa.text(f"DROP DATABASE IF EXISTS {db_name} WITH (FORCE)"))

    # Don't let stray executor/version pins leak between tests.
    for var in ("DBOS__VMID", "DBOS__APPVERSION", "DBOS__APPID"):
        os.environ.pop(var, None)


@pytest.fixture()
def dbos(
    config: DBOSConfig, cleanup_test_databases: None
) -> Generator[DBOS, Any, None]:
    """A launched DBOS instance on a fresh system database.

    Launches eagerly for convenience: tests register workflows/steps in their
    bodies and call them directly. Tests that must register before launch (e.g.
    recovery tests) should build their own instance from `config` +
    `cleanup_test_databases` instead.
    """
    DBOS.destroy(destroy_registry=True)
    instance = DBOS(config=config)
    DBOS.launch()
    yield instance
    DBOS.destroy(destroy_registry=True)


@pytest.fixture()
def dbos_client(dbos: DBOS) -> Generator[DBOSClient, Any, None]:
    """A DBOSClient against the same system database as the `dbos` fixture."""
    client = DBOSClient(system_database_url=system_database_url())
    yield client
    client.destroy()
