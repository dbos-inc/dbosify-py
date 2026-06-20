"""Shared fixtures for the adapted temporalio SDK conformance suites.

The ``client`` fixture (a ``Client`` over a client-mode ``DBOSClient`` pointed at
the freshly-(re)created test database) is defined here so every ``test_sdk_*.py``
module in this directory can request it.
"""

from typing import AsyncIterator

import pytest
import sqlalchemy as sa

from dbosify.client import Client
from tests.dbconfig import connect_client, system_database_url


def _ensure_database_exists() -> None:
    """The client-mode DBOSClient connects at construction, so the (freshly
    dropped) system database must exist before the Worker launches and migrates
    it (the Worker owns the schema)."""
    url = sa.make_url(system_database_url())
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


@pytest.fixture()
async def client(dbosify_env: None) -> AsyncIterator[Client]:
    _ensure_database_exists()
    client = await connect_client()
    try:
        yield client
    finally:
        await client.close()
