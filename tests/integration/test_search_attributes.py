"""Memo + search-attribute storage (DESIGN §6.2), backed by DBOS native
workflow attributes.

Covers the full surface: setting at start, reading back via ``describe()`` and
in-workflow ``info()``/``memo()``, upsert (set + ``value_unset``), continue-as-new
carry-forward and override, child workflows (explicit values, no inheritance),
and a PayloadCodec encrypting memo at rest.
"""

from contextlib import asynccontextmanager
from datetime import datetime, timezone
from typing import Any, AsyncIterator, Dict, List, Sequence

import pytest
from dbos import DBOSClient

from dbosify import workflow
from dbosify.client import Client
from dbosify.common import (
    SearchAttributeKey,
    SearchAttributePair,
    TypedSearchAttributes,
)
from dbosify.converter import DataConverter, Payload, PayloadCodec
from dbosify.worker import Worker
from tests.dbconfig import default_config, system_database_url

pytestmark = pytest.mark.usefixtures("dbosify_env")

TASK_QUEUE = "search-attr-tq"

KW = SearchAttributeKey.for_keyword("CustomKeyword")
NUM = SearchAttributeKey.for_int("CustomInt")
WHEN = SearchAttributeKey.for_datetime("CustomDatetime")
TAGS = SearchAttributeKey.for_keyword_list("CustomTags")


def _sa_repr(typed: TypedSearchAttributes) -> List[str]:
    return sorted(f"{p.key.name}={p.value!r}" for p in typed)


@workflow.defn
class HoldWorkflow:
    """Stays open until signalled. Captures what info()/memo() report at start
    (query handlers don't run on the workflow loop, so the run method does the
    capture) and exposes the captured values via queries."""

    def __init__(self) -> None:
        self.done = False
        self.captured_sa: List[str] = []
        self.captured_memo: Dict[str, Any] = {}

    @workflow.signal
    def finish(self) -> None:
        self.done = True

    @workflow.query
    def seen_sa(self) -> List[str]:
        return self.captured_sa

    @workflow.query
    def seen_memo(self) -> Dict[str, Any]:
        return self.captured_memo

    @workflow.run
    async def run(self) -> None:
        self.captured_sa = _sa_repr(workflow.info().typed_search_attributes)
        self.captured_memo = dict(workflow.memo())
        await workflow.wait_condition(lambda: self.done)


@workflow.defn
class UpsertWorkflow:
    @workflow.run
    async def run(self) -> List[str]:
        # Mutate both namespaces, then return what info() reflects in-run.
        workflow.upsert_search_attributes([KW.value_set("after"), NUM.value_set(7)])
        workflow.upsert_memo({"added": "yes", "drop_me": None})
        return _sa_repr(workflow.info().typed_search_attributes)


@workflow.defn
class UnsetWorkflow:
    @workflow.run
    async def run(self) -> List[str]:
        workflow.upsert_search_attributes([KW.value_unset()])
        return _sa_repr(workflow.info().typed_search_attributes)


@workflow.defn
class UpsertDictWorkflow:
    @workflow.run
    async def run(self) -> None:
        # Deprecated untyped-dict upsert form — must emit a DeprecationWarning.
        workflow.upsert_search_attributes({"CustomKeyword": ["dictform"]})


@workflow.defn
class UpsertBadDatetimeWorkflow:
    @workflow.run
    async def run(self) -> str:
        # A tz-naive datetime is invalid: the error surfaces synchronously at the
        # upsert call (catchable), not later in the deferred attribute write.
        try:
            workflow.upsert_search_attributes([WHEN.value_set(datetime(2026, 6, 16))])
        except ValueError:
            # State must be unchanged — the bad value was rejected before commit.
            assert len(workflow.info().typed_search_attributes) == 0
            return "rejected"
        return "accepted"


@workflow.defn
class CANCarryWorkflow:
    @workflow.run
    async def run(self, hop: bool) -> List[str]:
        if hop:
            workflow.continue_as_new(args=[False])
        return _sa_repr(workflow.info().typed_search_attributes)


@workflow.defn
class CANOverrideWorkflow:
    @workflow.run
    async def run(self, hop: bool) -> List[str]:
        if hop:
            workflow.continue_as_new(
                args=[False],
                search_attributes=TypedSearchAttributes(
                    [SearchAttributePair(NUM, 123)]
                ),
            )
        return _sa_repr(workflow.info().typed_search_attributes)


@workflow.defn
class ChildWorkflow:
    @workflow.run
    async def run(self) -> List[str]:
        return _sa_repr(workflow.info().typed_search_attributes)


@workflow.defn
class ParentWorkflow:
    @workflow.run
    async def run(self) -> Dict[str, List[str]]:
        base = workflow.info().workflow_id
        with_sa = await workflow.execute_child_workflow(
            ChildWorkflow.run,
            id=f"{base}_c1",
            search_attributes=TypedSearchAttributes([SearchAttributePair(NUM, 99)]),
        )
        # No search_attributes: a child must NOT inherit the parent's.
        without = await workflow.execute_child_workflow(
            ChildWorkflow.run, id=f"{base}_c2"
        )
        return {"with": with_sa, "without": without}


class ReverseCodec(PayloadCodec):
    """Toy stand-in for an encryption codec: reverses the payload bytes."""

    async def encode(self, payloads: Sequence[Payload]) -> List[Payload]:
        return [Payload(metadata=p.metadata, data=p.data[::-1]) for p in payloads]

    async def decode(self, payloads: Sequence[Payload]) -> List[Payload]:
        return [Payload(metadata=p.metadata, data=p.data[::-1]) for p in payloads]


@asynccontextmanager
async def _env(
    data_converter: DataConverter = DataConverter.default,
) -> AsyncIterator[Client]:
    worker = Worker(
        default_config(),
        task_queue=TASK_QUEUE,
        workflows=[
            HoldWorkflow,
            UpsertWorkflow,
            UnsetWorkflow,
            UpsertDictWorkflow,
            UpsertBadDatetimeWorkflow,
            CANCarryWorkflow,
            CANOverrideWorkflow,
            ChildWorkflow,
            ParentWorkflow,
        ],
        activities=[],
        data_converter=data_converter,
    )
    async with worker:
        dbos_client = DBOSClient(system_database_url=system_database_url())
        try:
            yield Client(dbos_client, data_converter=data_converter)
        finally:
            dbos_client.destroy()


async def test_start_sets_memo_and_search_attributes() -> None:
    when = datetime(2026, 6, 16, 12, 0, tzinfo=timezone.utc)
    typed = TypedSearchAttributes(
        [
            SearchAttributePair(KW, "hello"),
            SearchAttributePair(NUM, 42),
            SearchAttributePair(WHEN, when),
            SearchAttributePair(TAGS, ["a", "b"]),
        ]
    )
    async with _env() as client:
        handle = await client.start_workflow(
            HoldWorkflow.run,
            id="sa-start",
            task_queue=TASK_QUEUE,
            memo={"owner": "ada", "count": 3},
            search_attributes=typed,
        )
        # In-workflow info()/memo() see them.
        assert await handle.query(HoldWorkflow.seen_sa) == _sa_repr(typed)
        assert await handle.query(HoldWorkflow.seen_memo) == {
            "owner": "ada",
            "count": 3,
        }

        # describe() reads them back from the DBOS attributes column.
        desc = await handle.describe()
        assert desc.typed_search_attributes[KW] == "hello"
        assert desc.typed_search_attributes[NUM] == 42
        assert desc.typed_search_attributes[WHEN] == when
        assert desc.typed_search_attributes[TAGS] == ["a", "b"]
        # Legacy untyped view + memo accessors.
        assert desc.search_attributes["CustomKeyword"] == ["hello"]
        assert await desc.memo() == {"owner": "ada", "count": 3}
        assert await desc.memo_value("owner") == "ada"
        assert await desc.memo_value("missing", "fallback") == "fallback"

        await handle.signal(HoldWorkflow.finish)
        await handle.result()


async def test_upsert_memo_and_search_attributes() -> None:
    typed = TypedSearchAttributes([SearchAttributePair(KW, "before")])
    async with _env() as client:
        in_run = await client.execute_workflow(
            UpsertWorkflow.run,
            id="sa-upsert",
            task_queue=TASK_QUEUE,
            memo={"drop_me": "gone-soon", "keep": 1},
            search_attributes=typed,
        )
        # info() inside the run reflects the upsert immediately.
        assert in_run == ["CustomInt=7", "CustomKeyword='after'"]

        desc = await client.get_workflow_handle("sa-upsert").describe()
        assert desc.typed_search_attributes[KW] == "after"
        assert desc.typed_search_attributes[NUM] == 7
        memo = await desc.memo()
        assert memo == {"keep": 1, "added": "yes"}  # drop_me removed by None


async def test_upsert_value_unset_removes_key() -> None:
    typed = TypedSearchAttributes(
        [SearchAttributePair(KW, "doomed"), SearchAttributePair(NUM, 5)]
    )
    async with _env() as client:
        in_run = await client.execute_workflow(
            UnsetWorkflow.run,
            id="sa-unset",
            task_queue=TASK_QUEUE,
            search_attributes=typed,
        )
        assert in_run == ["CustomInt=5"]
        desc = await client.get_workflow_handle("sa-unset").describe()
        assert desc.typed_search_attributes.get(KW) is None
        assert desc.typed_search_attributes[NUM] == 5


async def test_continue_as_new_carries_attributes_forward() -> None:
    typed = TypedSearchAttributes(
        [SearchAttributePair(KW, "carried"), SearchAttributePair(NUM, 1)]
    )
    async with _env() as client:
        # run0 continues-as-new; run1 returns what it inherited.
        result = await client.execute_workflow(
            CANCarryWorkflow.run,
            True,
            id="sa-can",
            task_queue=TASK_QUEUE,
            memo={"m": "v"},
            search_attributes=typed,
        )
        assert result == ["CustomInt=1", "CustomKeyword='carried'"]
        # The chain's current run (run1) carries them on its column too.
        desc = await client.get_workflow_handle("sa-can").describe()
        assert desc.typed_search_attributes[KW] == "carried"
        assert await desc.memo() == {"m": "v"}


async def test_continue_as_new_overrides_search_attributes() -> None:
    typed = TypedSearchAttributes([SearchAttributePair(KW, "original")])
    async with _env() as client:
        result = await client.execute_workflow(
            CANOverrideWorkflow.run,
            True,
            id="sa-can-override",
            task_queue=TASK_QUEUE,
            search_attributes=typed,
        )
        # The override replaces the carried SA for the new run.
        assert result == ["CustomInt=123"]


async def test_child_explicit_attributes_and_no_inheritance() -> None:
    parent_sa = TypedSearchAttributes([SearchAttributePair(KW, "parent")])
    async with _env() as client:
        result = await client.execute_workflow(
            ParentWorkflow.run,
            id="sa-parent",
            task_queue=TASK_QUEUE,
            search_attributes=parent_sa,
        )
        assert result["with"] == ["CustomInt=99"]
        # A child with no search_attributes does NOT inherit the parent's.
        assert result["without"] == []


async def test_codec_encrypts_memo_at_rest() -> None:
    converter = DataConverter(payload_codec=ReverseCodec())
    async with _env(converter) as client:
        handle = await client.start_workflow(
            HoldWorkflow.run,
            id="sa-codec-memo",
            task_queue=TASK_QUEUE,
            memo={"secret": "classified"},
        )
        # In-workflow memo() decodes through the codec.
        assert await handle.query(HoldWorkflow.seen_memo) == {"secret": "classified"}
        desc = await handle.describe()
        assert await desc.memo() == {"secret": "classified"}
        await handle.signal(HoldWorkflow.finish)
        await handle.result()


async def test_dict_form_search_attributes_warn_at_start() -> None:
    # The deprecated untyped-dict form must emit a DeprecationWarning at the
    # client start path (proves the warning is wired, not just defined).
    async with _env() as client:
        with pytest.warns(DeprecationWarning):
            handle = await client.start_workflow(
                HoldWorkflow.run,
                id="sa-warn-start",
                task_queue=TASK_QUEUE,
                search_attributes={"CustomKeyword": ["x"]},
            )
        await handle.signal(HoldWorkflow.finish)
        await handle.result()


async def test_dict_form_search_attributes_warn_on_inworkflow_upsert() -> None:
    # ...and at the in-workflow upsert path.
    async with _env() as client:
        with pytest.warns(DeprecationWarning):
            await client.execute_workflow(
                UpsertDictWorkflow.run,
                id="sa-warn-upsert",
                task_queue=TASK_QUEUE,
            )


async def test_inworkflow_upsert_invalid_value_is_catchable() -> None:
    # An invalid value (tz-naive datetime) upserted in-workflow raises synchronously
    # at the call (catchable, state unchanged); the workflow catches it and returns.
    async with _env() as client:
        result = await client.execute_workflow(
            UpsertBadDatetimeWorkflow.run,
            id="sa-bad-datetime",
            task_queue=TASK_QUEUE,
        )
        assert result == "rejected"
