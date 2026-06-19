"""WorkflowEnvironment.start_local: an isolated throwaway database per
environment on the externally provided Postgres server, torn down on
shutdown (DESIGN §6.10).
"""

from datetime import timedelta

import pytest
import sqlalchemy

from dbosify import activity, workflow
from dbosify.testing import WorkflowEnvironment
from dbosify.worker import Worker
from tests.dbconfig import system_database_url

pytestmark = pytest.mark.usefixtures("dbosify_env")

TASK_QUEUE = "env-tq"


@activity.defn
async def env_greet(name: str) -> str:
    return f"Hello, {name}!"


@workflow.defn
class EnvGreeting:
    @workflow.run
    async def run(self, name: str) -> str:
        result: str = await workflow.execute_activity(
            env_greet, name, start_to_close_timeout=timedelta(seconds=10)
        )
        return result


def _database_exists(maintenance_url: str, name: str) -> bool:
    engine = sqlalchemy.create_engine(maintenance_url, isolation_level="AUTOCOMMIT")
    try:
        with engine.connect() as conn:
            row = conn.execute(
                sqlalchemy.text("SELECT 1 FROM pg_database WHERE datname = :n"),
                {"n": name},
            ).fetchone()
        return row is not None
    finally:
        engine.dispose()


async def test_workflow_environment_start_local() -> None:
    env = await WorkflowEnvironment.start_local(
        system_database_url=system_database_url()
    )
    env_url = env.dbos_config["system_database_url"]
    assert env_url is not None
    db_name = env_url.rsplit("/", 1)[1]
    maintenance_url = (
        sqlalchemy.make_url(system_database_url())
        .set(database="postgres")
        .render_as_string(hide_password=False)
    )
    async with env:
        # Its own database, not the one the URL named.
        assert db_name.startswith("dbosify_env_")
        assert _database_exists(maintenance_url, db_name)
        assert not env.supports_time_skipping

        worker = Worker(
            env.dbos_config,
            task_queue=TASK_QUEUE,
            workflows=[EnvGreeting],
            activities=[env_greet],
        )
        async with worker:
            result = await env.client.execute_workflow(
                EnvGreeting.run, "env", id="env-wf", task_queue=TASK_QUEUE
            )
        assert result == "Hello, env!"

    # Shutdown dropped the environment's database.
    assert not _database_exists(maintenance_url, db_name)
