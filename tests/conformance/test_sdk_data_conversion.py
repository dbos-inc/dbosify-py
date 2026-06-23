"""Conformance: data-conversion (codec), error-category, and worker-interceptor
tests adapted from temporalio's ``tests/worker/test_workflow.py``.

temporalio rebuilds the client via ``client.config()`` to install a codec; we
forward the ``DataConverter`` to ``new_worker`` instead — the converter is
process-global, so it applies to the client too. Server-only log
assertions (e.g. "Completing activity as failed") are dropped; the behavioral
core (codec round-trips, error category survives, an interceptor may register a
signal handler in ``init()``) is kept.
"""

from datetime import timedelta
from typing import Any, List, Sequence

import pytest

from dbosify import activity, workflow
from dbosify.client import Client, WorkflowFailureError
from dbosify.common import RetryPolicy
from dbosify.converter import DataConverter, Payload, PayloadCodec
from dbosify.exceptions import ActivityError, ApplicationError, ApplicationErrorCategory
from dbosify.worker import (
    Interceptor,
    WorkflowInboundInterceptor,
    WorkflowInterceptorClassInput,
    WorkflowOutboundInterceptor,
)

from .sdk_harness import HelloWorkflow, new_worker, wid


class PassThroughCodec(PayloadCodec):
    async def encode(self, payloads: Sequence[Payload]) -> List[Payload]:
        return list(payloads)

    async def decode(self, payloads: Sequence[Payload]) -> List[Payload]:
        return list(payloads)


def _codec_converter() -> DataConverter:
    return DataConverter(payload_codec=PassThroughCodec())


# ---------------------------------------------------------------------------
# Passthrough codec
# ---------------------------------------------------------------------------


@activity.defn
async def codec_greeting(name: str) -> str:
    return f"Hello, {name}!"


@workflow.defn
class CodecActivityWorkflow:
    @workflow.run
    async def run(self, name: str) -> str:
        result: str = await workflow.execute_activity(
            codec_greeting,
            name,
            start_to_close_timeout=timedelta(seconds=10),
        )
        return result


async def test_workflow_with_passthrough_codec(client: Client) -> None:
    # A no-op codec must round-trip activity args/results unchanged (regression:
    # the codec used to be unable to reuse the passed-in payloads).
    async with new_worker(
        client,
        CodecActivityWorkflow,
        activities=[codec_greeting],
        data_converter=_codec_converter(),
    ) as worker:
        result = await client.execute_workflow(
            CodecActivityWorkflow.run,
            "World",
            id=wid(),
            task_queue=worker.task_queue,
        )
    assert result == "Hello, World!"


@workflow.defn
class MemoDecodingWorkflow:
    @workflow.run
    async def run(self, memo_key: str) -> Any:
        return workflow.memo_value(memo_key)


async def test_workflow_memo_decoding_with_passthrough_codec(client: Client) -> None:
    # Memo decoding through a codec must not clobber the payload's encoding
    # metadata (a memory-sharing regression upstream).
    async with new_worker(
        client, MemoDecodingWorkflow, data_converter=_codec_converter()
    ) as worker:
        memo_value = await client.execute_workflow(
            MemoDecodingWorkflow.run,
            "memokey",
            id=wid(),
            task_queue=worker.task_queue,
            memo={"memokey": {"memoval_key": "memoval_value"}},
        )
    assert memo_value == {"memoval_key": "memoval_value"}


# ---------------------------------------------------------------------------
# ApplicationErrorCategory round-trip
# ---------------------------------------------------------------------------


@activity.defn
async def raise_categorized_error(use_benign: bool) -> None:
    if use_benign:
        raise ApplicationError(
            "This is a benign error", category=ApplicationErrorCategory.BENIGN
        )
    raise ApplicationError(
        "This is a regular error", category=ApplicationErrorCategory.UNSPECIFIED
    )


@workflow.defn
class RaiseCategorizedErrorWorkflow:
    @workflow.run
    async def run(self, use_benign: bool) -> None:
        await workflow.execute_activity(
            raise_categorized_error,
            use_benign,
            start_to_close_timeout=timedelta(seconds=5),
            retry_policy=RetryPolicy(maximum_attempts=1),
        )


async def test_activity_error_category_round_trips(client: Client) -> None:
    async with new_worker(
        client, RaiseCategorizedErrorWorkflow, activities=[raise_categorized_error]
    ) as worker:
        for use_benign, expected in [
            (True, ApplicationErrorCategory.BENIGN),
            (False, ApplicationErrorCategory.UNSPECIFIED),
        ]:
            with pytest.raises(WorkflowFailureError) as err:
                await client.execute_workflow(
                    RaiseCategorizedErrorWorkflow.run,
                    use_benign,
                    id=wid(),
                    task_queue=worker.task_queue,
                )
            assert isinstance(err.value.cause, ActivityError)
            assert isinstance(err.value.cause.cause, ApplicationError)
            assert err.value.cause.cause.category == expected


# ---------------------------------------------------------------------------
# Worker interceptor registering a signal handler in init()
# ---------------------------------------------------------------------------


class _SignalInboundInterceptor(WorkflowInboundInterceptor):
    def init(self, outbound: WorkflowOutboundInterceptor) -> None:
        # An interceptor may register a handler during init without breaking
        # execution (mirrors temporalio's SignalInboundInterceptor).
        workflow.set_signal_handler("my_random_signal", lambda: None)
        super().init(outbound)


class _SignalInterceptor(Interceptor):
    def workflow_interceptor_class(
        self, input: WorkflowInterceptorClassInput
    ) -> type[WorkflowInboundInterceptor]:
        return _SignalInboundInterceptor


async def test_signal_handler_in_interceptor(client: Client) -> None:
    async with new_worker(
        client, HelloWorkflow, interceptors=[_SignalInterceptor()]
    ) as worker:
        result = await client.execute_workflow(
            HelloWorkflow.run,
            "Temporal",
            id=wid(),
            task_queue=worker.task_queue,
        )
    assert result == "Hello, Temporal!"
