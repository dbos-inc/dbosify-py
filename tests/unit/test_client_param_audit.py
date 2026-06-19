"""Accepted-parameter audit for ``Client`` (DESIGN §9), mirroring the Worker
audit. Every parameter of ``temporalio.client.Client.connect`` / ``__init__`` is
classified — honored, subsumed by the ``DBOSClient`` we wrap (connection-surface), or a
fundamental deviation. Unlike the Worker, our ``Client`` has no ``**kwargs``
catch-all, so a temporalio connection option we don't accept raises a loud
``TypeError`` rather than being silently swallowed; the audit asserts exactly
that. The completeness check is machine-enforced: a new temporalio Client
parameter fails this test until it is classified.
"""

from typing import Dict, Mapping, Set

import pytest
import temporalio.client

from dbosify.client import Client
from tests.unit._param_audit import (
    Bucket,
    assert_buckets_disjoint,
    assert_every_param_classified,
    assert_honored_are_named,
    assert_reasons_present,
    named_params,
)

NAME = "client.Client.connect"

# We accept it as a named parameter and act on it.
HONORED: Set[str] = {
    "namespace",
    "data_converter",
    "interceptors",
    "default_workflow_query_reject_condition",
}

# The connection itself — ``target_host`` is replaced by ``connect``'s
# ``system_database_url``, ``service_client`` by the ``dbos_client`` the
# constructor takes (connection-surface).
DEVIATION: Set[str] = {"target_host", "service_client"}

# Carried by the DBOSClient you pass, or with no analog (no Temporal server /
# gRPC). Not accepted by our Client → passing one raises TypeError (never
# silently ignored). Each with a defensible reason.
SUBSUMED: Dict[str, str] = {
    "api_key": "no Temporal-server auth (no-server)",
    "plugins": "Client plugins not supported; use interceptors=",
    "tls": "connection security is the DBOSClient's Postgres connection (connection-surface)",
    "retry_config": "gRPC RPC retry; no Temporal gRPC (no-server)",
    "keep_alive_config": "gRPC keep-alive; no Temporal gRPC (no-server)",
    "rpc_metadata": "gRPC call metadata; no Temporal gRPC (no-server)",
    "identity": "client identity is a Temporal-server visibility concept; no server (no-server)",
    "lazy": "the DBOSClient is built eagerly; no lazy gRPC connection (connection-surface)",
    "runtime": "telemetry/metrics runtime not provided (no-metrics)",
    "http_connect_proxy_config": "gRPC HTTP proxy; no Temporal gRPC (no-server)",
    "dns_load_balancing_config": "gRPC DNS load balancing; no Temporal gRPC (no-server)",
    "header_codec_behavior": "header codec application follows our interceptor model",
}


# honored/deviation are sets; subsumed is {param: reason}.
BUCKETS: Mapping[str, Bucket] = {
    "honored": HONORED,
    "deviation": DEVIATION,
    "subsumed": SUBSUMED,
}


def _temporalio_client_params() -> Set[str]:
    # The full surface across both public constructors.
    return named_params(temporalio.client.Client.connect) | named_params(
        temporalio.client.Client.__init__
    )


def _our_client_params() -> Set[str]:
    # Drop our own connection machinery (the constructor's ``dbos_client`` and
    # connect's ``system_database_url``) — neither is a temporalio param.
    return (named_params(Client.connect) | named_params(Client.__init__)) - {
        "dbos_client",
        "system_database_url",
    }


def test_every_client_param_is_classified() -> None:
    assert_every_param_classified(NAME, _temporalio_client_params(), BUCKETS)


def test_buckets_are_disjoint() -> None:
    assert_buckets_disjoint(NAME, BUCKETS)


def test_subsumed_reasons_present() -> None:
    assert_reasons_present(NAME, BUCKETS)


def test_honored_params_are_accepted() -> None:
    assert_honored_are_named(NAME, HONORED, _our_client_params())


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
        Client.connect(None, api_key="k")  # type: ignore[call-arg,arg-type,unused-coroutine]
