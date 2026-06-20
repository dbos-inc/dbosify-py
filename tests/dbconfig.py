"""Database configuration shared by tests and subprocess worker scripts.

Integration tests need a running Postgres server, provisioned externally (CI
service container or a local installation). Tests never launch a server; they
drop and re-create their own databases on the provided one.

Connection configuration, in priority order:
1. ``DBOSIFY_TEST_SYSTEM_DATABASE_URL`` — full SQLAlchemy URL; the database it
   names is dropped/created by tests, so never point it at a database you
   care about.
2. ``PGHOST``/``PGPORT``/``PGUSER``/``PGPASSWORD`` (defaults: localhost,
   5432, postgres, dbos), with the test database name below.
"""

import os
from typing import TYPE_CHECKING
from urllib.parse import quote

import dbos
from dbos import DBOSConfig

from dbosify._internal.namespaces import DEFAULT_NAMESPACE, namespace_schema
from dbosify._internal.serializer import TEMPORAL_SERIALIZER

if TYPE_CHECKING:
    from dbosify.client import Client

TEST_SYSTEM_DB_NAME = "dbosify_test_dbos_sys"

# Every namespace maps to its own DBOS system schema (DEVIATIONS no-server).
# Tests run in the default namespace; all components must agree on its schema.
TEST_SCHEMA = namespace_schema(DEFAULT_NAMESPACE)

# Test clients are built via connect_client()/make_dbos_client() below, which set
# the JSON serializer + namespace schema explicitly — no DBOSClient monkeypatch.


def system_database_url() -> str:
    url = os.environ.get("DBOSIFY_TEST_SYSTEM_DATABASE_URL")
    if url is not None:
        return url
    host = os.environ.get("PGHOST", "localhost")
    port = os.environ.get("PGPORT", "5432")
    user = os.environ.get("PGUSER", "postgres")
    password = quote(os.environ.get("PGPASSWORD", "dbos"), safe="")
    return f"postgresql+psycopg://{user}:{password}@{host}:{port}/{TEST_SYSTEM_DB_NAME}"


def default_config() -> DBOSConfig:
    return {
        "name": "dbosify_test",
        "system_database_url": system_database_url(),
        "run_admin_server": False,
        # Speeds up recv/event delivery in tests.
        "notification_listener_polling_interval_sec": 0.01,
        # JSON transport (matches the Worker/Client and the raw-DBOS test
        # workers that build on default_config()).
        "serializer": TEMPORAL_SERIALIZER,
        # Default-namespace schema (DEVIATIONS no-server); raw-DBOS test drivers
        # that launch on this config land in the same schema as the product.
        "dbos_system_schema": TEST_SCHEMA,
    }


async def connect_client(namespace: str = DEFAULT_NAMESPACE) -> "Client":
    """A dbosify ``Client`` via the production ``Client.connect`` path — no
    hand-built ``DBOSClient``. ``Client.connect`` sets the JSON serializer and
    the namespace's schema itself; ``close()`` (or ``async with``) disposes it."""
    from dbosify.client import Client

    return await Client.connect(system_database_url(), namespace=namespace)


def make_dbos_client() -> "dbos.DBOSClient":
    """A raw ``DBOSClient`` for low-level test drivers (``send``/``list_workflows``/
    ``retrieve_workflow``) that the dbosify ``Client`` API can't express. The JSON
    serializer and default-namespace schema are set explicitly (no monkeypatch)."""
    return dbos.DBOSClient(
        system_database_url=system_database_url(),
        serializer=TEMPORAL_SERIALIZER,
        dbos_system_schema=TEST_SCHEMA,
    )
