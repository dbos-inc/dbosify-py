"""Subprocess worker for memo/search-attribute recovery.

Run as: python search_attr_recovery_worker.py <start|resume> <workflow_id>

The workflow starts with an initial memo + search attribute, upserts both
(changing a value, adding another, merging the memo), then sleeps so the
``update_workflow_attributes`` checkpoint is durable before it announces
"UPSERTED" and parks. The test SIGKILLs there. On resume, DBOS recovers the
parked run (re-decoding the initial attributes from the envelope and replaying
the upserts in memory; the write step replays from its checkpoint), the test
signals it to finish, and the worker reports the final attributes via
``describe()`` — proving the upserted memo/search attributes survived the crash.
"""

import asyncio
import json
import sys

from dbos import DBOSClient

from temporal_dbos import workflow
from temporal_dbos.client import Client
from temporal_dbos.common import (
    SearchAttributeKey,
    SearchAttributePair,
    TypedSearchAttributes,
)
from temporal_dbos.worker import Worker
from tests.dbconfig import default_config, system_database_url

TASK_QUEUE = "search-attr-recovery-tq"

KW = SearchAttributeKey.for_keyword("CustomKeyword")
NUM = SearchAttributeKey.for_int("CustomInt")


@workflow.defn
class UpsertRecoveryWorkflow:
    def __init__(self) -> None:
        self.done = False

    @workflow.signal
    def finish(self) -> None:
        self.done = True

    @workflow.run
    async def run(self) -> None:
        workflow.upsert_search_attributes([KW.value_set("survived"), NUM.value_set(7)])
        workflow.upsert_memo({"phase": "upserted"})
        # Yield so the upsert write checkpoints before we announce + park.
        await workflow.sleep(0.2)
        print("UPSERTED", flush=True)
        await workflow.wait_condition(lambda: self.done)


async def main() -> None:
    action, workflow_id = sys.argv[1], sys.argv[2]
    async with Worker(
        default_config(),
        task_queue=TASK_QUEUE,
        workflows=[UpsertRecoveryWorkflow],
        activities=[],
    ):
        dbos_client = DBOSClient(system_database_url=system_database_url())
        try:
            client = await Client.connect(dbos_client)
            if action == "start":
                await client.start_workflow(
                    UpsertRecoveryWorkflow.run,
                    id=workflow_id,
                    task_queue=TASK_QUEUE,
                    memo={"phase": "initial", "owner": "ada"},
                    search_attributes=TypedSearchAttributes(
                        [SearchAttributePair(KW, "initial")]
                    ),
                )
                print("STARTED", flush=True)
                # Park here; the workflow announces UPSERTED and waits. The test
                # SIGKILLs this process after seeing UPSERTED.
                await client.get_workflow_handle(workflow_id).result()
            else:
                assert action == "resume"
                handle = client.get_workflow_handle(workflow_id)
                await handle.signal(UpsertRecoveryWorkflow.finish)
                await handle.result()
                desc = await handle.describe()
                out = {
                    "sa": sorted(
                        f"{p.key.name}={p.value!r}"
                        for p in desc.typed_search_attributes
                    ),
                    "memo": await desc.memo(),
                }
                print("RESULT " + json.dumps(out), flush=True)
        finally:
            dbos_client.destroy()


if __name__ == "__main__":
    asyncio.run(main())
