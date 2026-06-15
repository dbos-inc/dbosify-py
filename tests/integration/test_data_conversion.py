"""Stage 2 (data conversion): workflow run arguments flow through the
DataConverter.

The crux: a typed run signature rebuilds the original Python type from the
payload, while an unannotated parameter comes back as a plain dict (matching
temporalio's default converter). A custom PayloadCodec transforms the stored
payload bytes and is reversed on the way back — the encryption use-case.
"""

from contextlib import asynccontextmanager
from dataclasses import dataclass
from typing import Any, AsyncIterator, List, Sequence

import pytest
from dbos import DBOSClient

from temporal_dbos import workflow
from temporal_dbos.client import Client
from temporal_dbos.converter import DataConverter, Payload, PayloadCodec
from temporal_dbos.worker import Worker
from tests.dbconfig import default_config, system_database_url

pytestmark = pytest.mark.usefixtures("tdb_env")

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
        workflows=[TypedArgWorkflow, UntypedArgWorkflow, TypedResultWorkflow],
        data_converter=data_converter,
    )
    async with worker:
        dbos_client = DBOSClient(system_database_url=system_database_url())
        try:
            yield await Client.connect(dbos_client, data_converter=data_converter)
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
