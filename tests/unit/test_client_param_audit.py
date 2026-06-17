"""Accepted-parameter audit for ``Client`` (DESIGN §9), mirroring the Worker
audit. Every parameter of ``temporalio.client.Client.connect`` / ``__init__`` is
classified — honored, subsumed by the ``DBOSClient`` we wrap (D2), or a
fundamental deviation. Unlike the Worker, our ``Client`` has no ``**kwargs``
catch-all, so a temporalio connection option we don't accept raises a loud
``TypeError`` rather than being silently swallowed; the audit asserts exactly
that. The completeness check is machine-enforced: a new temporalio Client
parameter fails this test until it is classified.
"""

import inspect
from typing import Dict, Set

import pytest
import temporalio.client

from temporal_dbos.client import Client

# We accept it as a named parameter and act on it.
HONORED: Set[str] = {
    "data_converter",
    "interceptors",
    "default_workflow_query_reject_condition",
}

# The connection itself — replaced by the ``dbos_client`` we take (D2).
DEVIATION: Set[str] = {"target_host", "service_client"}

# Carried by the DBOSClient you pass, or with no analog (no Temporal server /
# gRPC). Not accepted by our Client → passing one raises TypeError (never
# silently ignored). Each with a defensible reason.
SUBSUMED: Dict[str, str] = {
    "namespace": "namespacing rides on the DBOSClient's dbos_system_schema (D2)",
    "api_key": "no Temporal-server auth (D1)",
    "plugins": "Client plugins not supported; use interceptors= (D24)",
    "tls": "connection security is the DBOSClient's Postgres connection (D2)",
    "retry_config": "gRPC RPC retry; no Temporal gRPC (D1)",
    "keep_alive_config": "gRPC keep-alive; no Temporal gRPC (D1)",
    "rpc_metadata": "gRPC call metadata; no Temporal gRPC (D1)",
    "identity": "client identity is a Temporal-server visibility concept; no server (D1)",
    "lazy": "the DBOSClient is built eagerly; no lazy gRPC connection (D2)",
    "runtime": "telemetry/metrics runtime not provided (D33)",
    "http_connect_proxy_config": "gRPC HTTP proxy; no Temporal gRPC (D1)",
    "dns_load_balancing_config": "gRPC DNS load balancing; no Temporal gRPC (D1)",
    "header_codec_behavior": "header codec application follows our interceptor model (D24)",
}


def _params(fn: object) -> Set[str]:
    return {
        p.name
        for p in inspect.signature(fn).parameters.values()  # type: ignore[arg-type]
        if p.name != "self"
        and p.kind
        not in (inspect.Parameter.VAR_KEYWORD, inspect.Parameter.VAR_POSITIONAL)
    }


def _temporalio_client_params() -> Set[str]:
    # The full surface across both public constructors.
    return _params(temporalio.client.Client.connect) | _params(
        temporalio.client.Client.__init__
    )


def _our_client_params() -> Set[str]:
    return (_params(Client.connect) | _params(Client.__init__)) - {"dbos_client"}


def test_every_client_param_is_classified() -> None:
    actual = _temporalio_client_params()
    classified = HONORED | DEVIATION | set(SUBSUMED)

    unclassified = actual - classified
    assert not unclassified, (
        f"temporalio Client params not classified: {sorted(unclassified)} "
        "— add each to HONORED / SUBSUMED / DEVIATION."
    )
    stale = classified - actual
    assert (
        not stale
    ), f"audit classifies names that aren't temporalio Client params: {sorted(stale)}"


def test_buckets_are_disjoint() -> None:
    buckets = [HONORED, DEVIATION, set(SUBSUMED)]
    for i, a in enumerate(buckets):
        for b in buckets[i + 1 :]:
            assert not (a & b), f"param classified in two buckets: {sorted(a & b)}"


def test_subsumed_reasons_present() -> None:
    assert all(reason.strip() for reason in SUBSUMED.values())


def test_honored_params_are_accepted() -> None:
    ours = _our_client_params()
    not_accepted = HONORED - ours
    assert not not_accepted, f"honored but not a Client param: {sorted(not_accepted)}"


def test_unsupported_params_are_not_silently_accepted() -> None:
    # No **kwargs catch-all: every non-honored temporalio param is *not* a
    # parameter on our Client, so passing one raises TypeError (loud) rather
    # than being silently ignored.
    ours = _our_client_params()
    silently_accepted = (DEVIATION | set(SUBSUMED)) & ours
    assert (
        not silently_accepted
    ), f"these should not be accepted by our Client: {sorted(silently_accepted)}"


def test_passing_an_unsupported_option_raises_typeerror() -> None:
    # Bad kwargs raise at call time (before the coroutine is created/awaited).
    with pytest.raises(TypeError):
        Client.connect(None, namespace="prod")  # type: ignore[call-arg,arg-type,unused-coroutine]
