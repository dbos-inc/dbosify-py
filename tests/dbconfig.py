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
from typing import Any
from urllib.parse import quote

import dbos
from dbos import DBOSConfig

from dbosify._internal.namespaces import DEFAULT_NAMESPACE, namespace_schema
from dbosify._internal.serializer import TEMPORAL_SERIALIZER

TEST_SYSTEM_DB_NAME = "dbosify_test_dbos_sys"

# Every namespace maps to its own DBOS system schema (DEVIATIONS D1). Tests run
# in the default namespace, so all components — the product Worker/Client and
# the raw-DBOS test drivers — must agree on its schema.
TEST_SCHEMA = namespace_schema(DEFAULT_NAMESPACE)

# Every process touching the test database must use the JSON serializer (DBOS
# selects the deserializer by row label and rejects a mismatch). The Worker and
# Client install it on the product paths; this default covers the test-side raw
# DBOSClients (recovery drivers, chaos clients) — in both the pytest process and
# the subprocess workers, since both import this module.
_orig_dbos_client_init = dbos.DBOSClient.__init__


def _dbos_client_init(self: "dbos.DBOSClient", *args: Any, **kwargs: Any) -> None:
    kwargs.setdefault("serializer", TEMPORAL_SERIALIZER)
    kwargs.setdefault("dbos_system_schema", TEST_SCHEMA)
    _orig_dbos_client_init(self, *args, **kwargs)


dbos.DBOSClient.__init__ = _dbos_client_init  # type: ignore[method-assign]


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
        # Default-namespace schema (DEVIATIONS D1); matches Worker(namespace=
        # "default") and the patched DBOSClient above. Raw-DBOS test drivers
        # that launch on this config land in the same schema as the product.
        "dbos_system_schema": TEST_SCHEMA,
    }
