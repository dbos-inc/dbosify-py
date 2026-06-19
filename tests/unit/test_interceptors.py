"""Unit coverage for the interceptor header channel: the
wire-form header codec round-trip and the interceptor base-class delegation.
The end-to-end propagation is exercised under Postgres in
tests/integration/test_interceptors.py.
"""

import asyncio
from typing import Any, Dict, List, Sequence

from dbosify._internal import conversion
from dbosify._internal import workflow_interceptor as wfi
from dbosify.converter import Payload


def _payloads(*values: object) -> Dict[str, Any]:
    pc = conversion.get_converter().payload_converter
    return {f"k{i}": pc.to_payload(v) for i, v in enumerate(values)}


def test_encode_decode_headers_round_trip() -> None:
    headers = _payloads("hello", 42, {"nested": [1, 2]})
    wire = asyncio.run(conversion.encode_headers(headers))
    # Wire form is JSON-safe payload dicts (no raw Payload / bytes).
    assert all(isinstance(v, dict) for v in wire.values())

    decoded = asyncio.run(conversion.decode_headers(wire))
    assert set(decoded) == set(headers)
    assert all(isinstance(v, Payload) for v in decoded.values())
    pc = conversion.get_converter().payload_converter
    assert pc.from_payload(decoded["k0"]) == "hello"
    assert pc.from_payload(decoded["k1"]) == 42
    assert pc.from_payload(decoded["k2"]) == {"nested": [1, 2]}


def test_encode_decode_headers_empty() -> None:
    assert asyncio.run(conversion.encode_headers(None)) == {}
    assert asyncio.run(conversion.encode_headers({})) == {}
    assert asyncio.run(conversion.decode_headers(None)) == {}
    assert asyncio.run(conversion.decode_headers({})) == {}


def test_encode_headers_applies_codec() -> None:
    """A configured PayloadCodec transforms header bytes at rest and the
    round-trip restores them."""
    from dbosify.converter import DataConverter, PayloadCodec

    class _XorCodec(PayloadCodec):
        async def encode(self, payloads: Sequence[Payload]) -> List[Payload]:
            return [
                Payload(
                    metadata={**p.metadata, "codec": b"xor"},
                    data=bytes(b ^ 0x5A for b in p.data),
                )
                for p in payloads
            ]

        async def decode(self, payloads: Sequence[Payload]) -> List[Payload]:
            return [
                Payload(
                    metadata={k: v for k, v in p.metadata.items() if k != "codec"},
                    data=bytes(b ^ 0x5A for b in p.data),
                )
                for p in payloads
            ]

    conversion.set_converter(DataConverter(payload_codec=_XorCodec()))
    try:
        headers = _payloads("secret")
        plaintext = headers["k0"].data
        wire = asyncio.run(conversion.encode_headers(headers))
        # At rest the bytes are codec-transformed (not the plaintext payload).
        assert "json" not in wire["k0"], "codec'd headers must not inline plaintext"
        restored = asyncio.run(conversion.decode_headers(wire))
        assert restored["k0"].data == plaintext
    finally:
        conversion.reset_converter()


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
