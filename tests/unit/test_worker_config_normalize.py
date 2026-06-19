"""The Worker accepts either a Postgres URL or a full ``DBOSConfig`` as its
first argument, and forces the DBOS admin server off either way (DBOSify exposes
DBOS's management APIs, not the admin HTTP port — DESIGN §1). These cover
``_normalize_config`` without launching DBOS.
"""

from dbos import DBOSConfig

from dbosify.worker import DEFAULT_APP_NAME, _normalize_config

URL = "postgresql+psycopg://u:p@localhost:5432/db"


def test_url_string_expands_to_config() -> None:
    config = _normalize_config(URL)
    assert config["name"] == DEFAULT_APP_NAME
    assert config["system_database_url"] == URL
    assert config["run_admin_server"] is False


def test_config_passed_through() -> None:
    given: DBOSConfig = {"name": "app", "system_database_url": URL, "executor_id": "e1"}
    config = _normalize_config(given)
    assert config["name"] == "app"
    assert config["system_database_url"] == URL
    assert config["executor_id"] == "e1"
    assert config["run_admin_server"] is False


def test_admin_server_forced_off_even_if_requested() -> None:
    given: DBOSConfig = {"name": "app", "run_admin_server": True}
    assert _normalize_config(given)["run_admin_server"] is False


def test_does_not_mutate_caller_config() -> None:
    given: DBOSConfig = {"name": "app"}
    _normalize_config(given)
    assert "run_admin_server" not in given
