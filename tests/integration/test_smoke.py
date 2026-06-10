"""Smoke tests proving the test infrastructure itself: a provisioned Postgres
is reachable, databases are recreated per test, and DBOS workflows round-trip
both sync and async. If these fail, fix the environment before debugging
anything else.
"""

from dbos import DBOS, DBOSClient


def test_sync_workflow_roundtrip(dbos: DBOS) -> None:
    @DBOS.step()
    def add_step(x: int, y: int) -> int:
        return x + y

    @DBOS.workflow()
    def add_workflow(x: int, y: int) -> int:
        return add_step(x, y)

    handle = DBOS.start_workflow(add_workflow, 2, 3)
    assert handle.get_result() == 5


async def test_async_workflow_roundtrip(dbos: DBOS) -> None:
    @DBOS.step()
    async def mul_step(x: int, y: int) -> int:
        return x * y

    @DBOS.workflow()
    async def mul_workflow(x: int, y: int) -> int:
        return await mul_step(x, y)

    handle = await DBOS.start_workflow_async(mul_workflow, 2, 3)
    assert await handle.get_result() == 6


def test_client_sees_workflows(dbos: DBOS, dbos_client: DBOSClient) -> None:
    @DBOS.workflow()
    def noop_workflow() -> str:
        return "done"

    handle = DBOS.start_workflow(noop_workflow)
    assert handle.get_result() == "done"

    statuses = dbos_client.list_workflows()
    assert [s.workflow_id for s in statuses] == [handle.workflow_id]
