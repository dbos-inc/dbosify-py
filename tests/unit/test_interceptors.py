"""Unit coverage for the interceptor header channel (DEVIATIONS D24): the
wire-form header codec round-trip and the interceptor base-class delegation.
The end-to-end propagation is exercised under Postgres in
tests/integration/test_interceptors.py.
"""

import asyncio
from typing import Any, Dict, List

from temporal_dbos._internal import conversion
from temporal_dbos._internal import workflow_interceptor as wfi
from temporal_dbos.converter import Payload


def _payloads(*values: object) -> Dict[str, Any]:
    pc = conversion.get_converter().payload_converter
    return {f"k{i}": pc.to_payload(v) for i, v in enumerate(values)}


def test_encode_decode_headers_round_trip() -> None:
    headers = _payloads("hello", 42, {"nested": [1, 2]})
    wire = conversion.encode_headers(headers)
    # Wire form is JSON-safe payload dicts (no raw Payload / bytes).
    assert all(isinstance(v, dict) for v in wire.values())

    decoded = conversion.decode_headers(wire)
    assert set(decoded) == set(headers)
    assert all(isinstance(v, Payload) for v in decoded.values())
    pc = conversion.get_converter().payload_converter
    assert pc.from_payload(decoded["k0"]) == "hello"
    assert pc.from_payload(decoded["k1"]) == 42
    assert pc.from_payload(decoded["k2"]) == {"nested": [1, 2]}


def test_encode_decode_headers_empty() -> None:
    assert conversion.encode_headers(None) == {}
    assert conversion.encode_headers({}) == {}
    assert conversion.decode_headers(None) == {}
    assert conversion.decode_headers({}) == {}


async def _noop_run(*args: Any) -> str:
    return "ran"


def test_inbound_base_delegates_to_next() -> None:
    calls: List[str] = []

    class Recording(wfi.WorkflowInboundInterceptor):
        def __init__(self) -> None:  # chain root: no next
            pass

        async def execute_workflow(self, input: wfi.ExecuteWorkflowInput) -> object:
            calls.append("execute_workflow")
            return "ok"

        def init(self, outbound: wfi.WorkflowOutboundInterceptor) -> None:
            calls.append("init")

    # A default interceptor wrapping the root delegates every verb through.
    chain = wfi.WorkflowInboundInterceptor(Recording())
    chain.init(wfi.WorkflowOutboundInterceptor(_NoopOutbound()))

    async def _drive() -> object:
        return await chain.execute_workflow(
            wfi.ExecuteWorkflowInput(type=object, run_fn=_noop_run, args=[], headers={})
        )

    assert asyncio.run(_drive()) == "ok"
    assert calls == ["init", "execute_workflow"]


class _NoopOutbound(wfi.WorkflowOutboundInterceptor):
    def __init__(self) -> None:  # chain root
        pass
