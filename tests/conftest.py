"""Shared test fixtures. Database connection settings live in tests/dbconfig.py
(a separate module so subprocess worker scripts can import them too).
"""

import os
from typing import Any, Generator

import pytest
import sqlalchemy as sa
from dbos import DBOS, DBOSClient, DBOSConfig

from tests.dbconfig import default_config, make_dbos_client, system_database_url


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
    """A DBOSClient against the same system database as the `dbos` fixture,
    with the JSON serializer + namespace schema set explicitly (make_dbos_client)."""
    client = make_dbos_client()
    yield client
    client.destroy()


@pytest.fixture()
def dbosify_env(cleanup_test_databases: None) -> Generator[None, Any, None]:
    """A clean slate for public-API (Client/Worker) tests: fresh database;
    the Worker owns the DBOS lifecycle itself.
    """
    from dbosify import worker

    worker._reset_for_tests()
    yield
    worker._reset_for_tests()


@pytest.fixture()
def dbosify(dbos: DBOS) -> DBOS:
    """A launched DBOS plus clean dbosify registries.

    The dbosify registries are module-global (per-process, like real
    worker processes), while the `dbos` fixture destroys and re-creates the
    DBOS registry per test — so cached per-type dispatchers would point at a
    destroyed registry. Reset ours to match.
    """
    from dbosify._internal import dispatcher

    dispatcher._reset_for_tests()
    return dbos
