"""Data conversion: workflow run arguments flow through the
DataConverter.

The crux: a typed run signature rebuilds the original Python type from the
payload, while an unannotated parameter comes back as a plain dict (matching
temporalio's default converter). A custom PayloadCodec transforms the stored
payload bytes and is reversed on the way back — the encryption use-case.
"""

from contextlib import asynccontextmanager
from dataclasses import dataclass
from datetime import timedelta
from typing import Any, AsyncIterator, List, Sequence

import pytest
from dbos import DBOSClient

from dbosify import activity, workflow
from dbosify.client import Client
from dbosify.converter import DataConverter, Payload, PayloadCodec
from dbosify.worker import Worker
from tests.dbconfig import default_config, system_database_url

pytestmark = pytest.mark.usefixtures("dbosify_env")

TASK_QUEUE = "conversion-tq"


@dataclass
class GreetRequest:
    greeting: str
    name: str


@workflow.defn
class TypedArgWorkflow:
    @workflow.run
    async def run(self, req: GreetRequest) -> str:
        # Attribute access only works if req was rebuilt into a GreetRequest.
        assert isinstance(req, GreetRequest)
        return f"{req.greeting}, {req.name}"


@workflow.defn
class UntypedArgWorkflow:
    @workflow.run
    async def run(self, req) -> str:  # type: ignore[no-untyped-def]
        # No annotation -> no type hint -> a plain dict, as in temporalio.
        return type(req).__name__


@workflow.defn
class TypedResultWorkflow:
    @workflow.run
    async def run(self) -> GreetRequest:
        return GreetRequest(greeting="Yo", name="Cy")


@workflow.defn
class UpdateQueryWorkflow:
    def __init__(self) -> None:
        self.done = False

    @workflow.signal
    def finish(self) -> None:
        self.done = True

    @workflow.update
    def transform_update(self, req: GreetRequest) -> GreetRequest:
        return GreetRequest(greeting=req.greeting.upper(), name=req.name)

    @workflow.query
    def echo_query(self, req: GreetRequest) -> GreetRequest:
        return req

    @workflow.run
    async def run(self) -> None:
        await workflow.wait_condition(lambda: self.done)


@workflow.defn
class DefaultArgWorkflow:
    @workflow.run
    async def run(
        self,
        req: GreetRequest,
        extra: GreetRequest = GreetRequest(greeting="def", name="ault"),
    ) -> str:
        # Two typed params called with one arg: per-position slicing keeps the
        # hint for the arg present (extra uses its default).
        assert isinstance(req, GreetRequest)
        assert isinstance(extra, GreetRequest)
        return f"{req.greeting} {extra.name}"


@activity.defn
async def transform(req: GreetRequest) -> GreetRequest:
    # Attribute access works only if the dataclass arg was reconstructed.
    return GreetRequest(greeting=req.greeting.upper(), name=req.name)


@activity.defn
async def make_greeting() -> dict:  # type: ignore[type-arg]
    # Annotated to return a bare dict: only an explicit
    # execute_activity(result_type=) override reconstructs a GreetRequest.
    return {"greeting": "Hi", "name": "Ovr"}


@workflow.defn
class ResultTypeOverrideWorkflow:
    @workflow.run
    async def run(self) -> str:
        res = await workflow.execute_activity(
            make_greeting,
            start_to_close_timeout=timedelta(seconds=10),
            result_type=GreetRequest,
        )
        # result_type wins over the activity's registered `-> dict` return.
        assert isinstance(res, GreetRequest), type(res).__name__
        return f"{res.greeting}/{res.name}"


@workflow.defn
class ActivityRoundtripWorkflow:
    @workflow.run
    async def run(self, req: GreetRequest) -> GreetRequest:
        result: GreetRequest = await workflow.execute_activity(
            transform, req, start_to_close_timeout=timedelta(seconds=10)
        )
        return result


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
            TypedArgWorkflow,
            UntypedArgWorkflow,
            TypedResultWorkflow,
            ActivityRoundtripWorkflow,
            UpdateQueryWorkflow,
            DefaultArgWorkflow,
            ResultTypeOverrideWorkflow,
        ],
        activities=[transform, make_greeting],
        data_converter=data_converter,
    )
    async with worker:
        dbos_client = DBOSClient(system_database_url=system_database_url())
        try:
            yield Client(dbos_client, data_converter=data_converter)
        finally:
            dbos_client.destroy()


async def test_typed_dataclass_arg_is_reconstructed() -> None:
    async with _env() as client:
        result = await client.execute_workflow(
            TypedArgWorkflow.run,
            GreetRequest(greeting="Hello", name="Ada"),
            id="typed-arg",
            task_queue=TASK_QUEUE,
        )
    assert result == "Hello, Ada"


async def test_untyped_arg_is_plain_dict() -> None:
    async with _env() as client:
        result = await client.execute_workflow(
            UntypedArgWorkflow.run,
            GreetRequest(greeting="Hi", name="Bo"),
            id="untyped-arg",
            task_queue=TASK_QUEUE,
        )
    # Without a type hint the dataclass round-trips as a dict (deviation #12).
    assert result == "dict"


async def test_typed_result_is_reconstructed() -> None:
    # The result type is inferred from the run method's return annotation, so
    # the dataclass comes back reconstructed (not a dict).
    async with _env() as client:
        result = await client.execute_workflow(
            TypedResultWorkflow.run, id="typed-result", task_queue=TASK_QUEUE
        )
    assert result == GreetRequest("Yo", "Cy")
    assert isinstance(result, GreetRequest)


async def test_activity_dataclass_roundtrip() -> None:
    # Dataclass arg -> activity (reconstructed) -> dataclass result ->
    # workflow (reconstructed) -> client (reconstructed), end to end.
    async with _env() as client:
        result = await client.execute_workflow(
            ActivityRoundtripWorkflow.run,
            GreetRequest(greeting="hi", name="Di"),
            id="activity-roundtrip",
            task_queue=TASK_QUEUE,
        )
    assert result == GreetRequest(greeting="HI", name="Di")
    assert isinstance(result, GreetRequest)


async def test_typed_update_and_query_results() -> None:
    # Dataclass args reconstructed in the handlers; dataclass results
    # reconstructed at the client (result type inferred from the handler).
    async with _env() as client:
        handle = await client.start_workflow(
            UpdateQueryWorkflow.run, id="update-query", task_queue=TASK_QUEUE
        )
        upd = await handle.execute_update(
            UpdateQueryWorkflow.transform_update, GreetRequest("hey", "Al")
        )
        assert upd == GreetRequest("HEY", "Al") and isinstance(upd, GreetRequest)
        q = await handle.query(UpdateQueryWorkflow.echo_query, GreetRequest("yo", "Em"))
        assert q == GreetRequest("yo", "Em") and isinstance(q, GreetRequest)
        await handle.signal(UpdateQueryWorkflow.finish)
        await handle.result()


async def test_default_valued_arg_keeps_per_position_hint() -> None:
    # Called with one arg against a two-typed-param signature: the provided arg
    # is still reconstructed (the second param falls back to its default).
    async with _env() as client:
        result = await client.execute_workflow(
            DefaultArgWorkflow.run,
            GreetRequest(greeting="Hello", name="Ada"),
            id="default-arg",
            task_queue=TASK_QUEUE,
        )
    assert result == "Hello ault"


async def test_execute_activity_result_type_override() -> None:
    # execute_activity(result_type=GreetRequest) reconstructs the activity's
    # bare-dict result, overriding the registry's `-> dict` return annotation.
    async with _env() as client:
        result = await client.execute_workflow(
            ResultTypeOverrideWorkflow.run,
            id="result-type-override",
            task_queue=TASK_QUEUE,
        )
    assert result == "Hi/Ovr"


async def test_custom_codec_roundtrips_args() -> None:
    # A codec transforms the payload bytes at rest and reverses them on the
    # way back; the workflow still receives the correct reconstructed value.
    converter = DataConverter(payload_codec=ReverseCodec())
    async with _env(converter) as client:
        result = await client.execute_workflow(
            TypedArgWorkflow.run,
            GreetRequest(greeting="Encrypted", name="Zoe"),
            id="codec-arg",
            task_queue=TASK_QUEUE,
        )
    assert result == "Encrypted, Zoe"
