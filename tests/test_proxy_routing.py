"""Routing decision table for the unified proxy selection.

Every NO_PROXY token kind (IPv4/IPv6 literal, CIDR range, leading-dot
domain, port-qualified and scheme-qualified tokens) is traced here to a
direct or proxied routing decision, together with the proxy credentials
that may (or may not) be attached to the outgoing request.

These tests use a fully mocked transport so the decision table runs
without any network access; end-to-end coverage with a recording proxy
server lives in test_proxy_functional.py.
"""

from collections.abc import AsyncIterator, Callable
from http.cookies import SimpleCookie
from types import MappingProxyType
from typing import Any
from unittest import mock

import pytest
from multidict import CIMultiDict
from yarl import URL

import aiohttp
from aiohttp import hdrs, helpers
from aiohttp.client import ClientSession
from aiohttp.client_proto import ResponseHandler
from aiohttp.client_reqrep import ClientTimeout
from aiohttp.connector import BaseConnector, UnixConnector
from aiohttp.payload import Payload

_PROXY_URL = "http://proxy.example:8080"
_PROXY_AUTH = helpers.encode_basic_auth("user", "pass")


@pytest.fixture
def proxy_env(
    monkeypatch: pytest.MonkeyPatch,
) -> dict[str, str]:
    """Provide a hermetic, mutable proxy environment.

    The returned dict stands in for the process environment; sessions
    snapshot it at creation time, so tests can verify that later
    mutations do not affect existing sessions.
    """
    env: dict[str, str] = {}

    def fake_getproxies() -> dict[str, str]:
        # Mirror urllib.request.getproxies: keys lose the "_proxy" suffix.
        return {
            k[: -len("_proxy")] if k.endswith("_proxy") else k: v
            for k, v in env.items()
        }

    monkeypatch.setattr(helpers, "getproxies", fake_getproxies)
    return env


def _make_response(
    status: int = 200,
    headers: dict[str, str] | None = None,
    url: str = "http://example.com/",
) -> Any:
    resp = mock.Mock()
    resp.status = status
    resp.method = "GET"
    resp.headers = CIMultiDict(headers or {})
    resp.url = URL(url)
    resp.cookies = SimpleCookie()
    resp._raw_cookie_headers = None
    resp.connection = None
    resp.start = mock.AsyncMock()
    resp.release = mock.Mock()
    resp.close = mock.Mock()
    return resp


class _RoutingHarness:
    """Records the proxy routing decisions of a ClientSession."""

    def __init__(
        self,
        proto: ResponseHandler,
        responses: list[Any],
        connector: BaseConnector | None = None,
        **session_kwargs: Any,
    ) -> None:
        self.recorded: list[dict[str, Any]] = []
        self._responses = responses
        self.connections: list[Any] = []
        responses_iter = iter(responses)

        def req_factory(*args: Any, **kwargs: Any) -> Any:
            self.recorded.append(kwargs)
            req = mock.create_autospec(aiohttp.ClientRequest, spec_set=True)
            req._body = mock.create_autospec(Payload, spec_set=True, instance=True)
            req._body.consumed = False
            req._timeout = ClientTimeout()
            req._traces = []
            req._response_params = {}
            req._session = kwargs["session"]
            req._send = mock.AsyncMock(
                side_effect=lambda *a: next(responses_iter)
            )
            return req

        connector = connector or BaseConnector()
        connector._create_connection = mock.AsyncMock(  # type: ignore[method-assign]
            return_value=proto
        )
        original_connect = connector.connect
        connections = self.connections

        async def connect(req: Any, traces: Any, timeout: Any) -> Any:
            conn = await original_connect(req, traces=traces, timeout=timeout)
            connections.append(conn)
            return conn

        connector.connect = connect  # type: ignore[method-assign]
        self.session = ClientSession(
            connector=connector,
            request_class=req_factory,
            **session_kwargs,
        )

    def assert_routing(
        self,
        index: int,
        expected_proxy: str | None,
        expected_auth: str | None = None,
    ) -> None:
        """Assert the proxy and credentials routed for request *index*."""
        call = self.recorded[index]
        expected = URL(expected_proxy) if expected_proxy is not None else None
        assert call["proxy"] == expected
        proxy_headers = call["proxy_headers"] or CIMultiDict()
        if expected_auth is None:
            assert hdrs.PROXY_AUTHORIZATION not in proxy_headers
        else:
            assert proxy_headers[hdrs.PROXY_AUTHORIZATION] == expected_auth


@pytest.fixture
async def routing(
    create_mocked_conn: Callable[[], ResponseHandler],
) -> AsyncIterator[Callable[..., _RoutingHarness]]:
    harnesses: list[_RoutingHarness] = []

    def factory(
        responses: list[Any] | None = None,
        connector: BaseConnector | None = None,
        **session_kwargs: Any,
    ) -> _RoutingHarness:
        harness = _RoutingHarness(
            create_mocked_conn(),
            responses if responses is not None else [_make_response()],
            connector,
            **session_kwargs,
        )
        harnesses.append(harness)
        return harness

    yield factory
    for harness in harnesses:
        for conn in harness.connections:
            conn.release()
        await harness.session.close()


@pytest.mark.parametrize(
    ("env", "url", "expected_proxy"),
    (
        # No proxy configured -> direct.
        ({}, "http://example.com/", None),
        # Scheme-based proxy selection.
        ({"http_proxy": _PROXY_URL}, "http://example.com/", _PROXY_URL),
        ({"http_proxy": _PROXY_URL}, "https://example.com/", None),
        ({"https_proxy": _PROXY_URL}, "https://example.com/", _PROXY_URL),
        ({"https_proxy": _PROXY_URL}, "http://example.com/", None),
        # IPv4 literal token.
        ({"no_proxy": "10.0.0.1"}, "http://10.0.0.1/", None),
        ({"no_proxy": "10.0.0.1"}, "http://10.0.0.2/", _PROXY_URL),
        # IPv6 literal tokens, bare and bracketed.
        ({"no_proxy": "::1"}, "http://[::1]/", None),
        ({"no_proxy": "[::1]"}, "http://[::1]/", None),
        ({"no_proxy": "::1"}, "http://[::2]/", _PROXY_URL),
        # IPv4/IPv6 CIDR ranges.
        ({"no_proxy": "10.0.0.0/8"}, "http://10.1.2.3/", None),
        ({"no_proxy": "10.0.0.0/8"}, "http://11.0.0.1/", _PROXY_URL),
        ({"no_proxy": "fd00::/8"}, "http://[fd00::1]/", None),
        ({"no_proxy": "fd00::/8"}, "http://10.0.0.1/", _PROXY_URL),
        # A CIDR range never matches a hostname.
        ({"no_proxy": "192.168.0.0/16"}, "http://example.com/", _PROXY_URL),
        # Plain and leading-dot domains match the domain and subdomains.
        ({"no_proxy": "example.com"}, "http://example.com/", None),
        ({"no_proxy": "example.com"}, "http://www.example.com/", None),
        ({"no_proxy": ".example.com"}, "http://example.com/", None),
        ({"no_proxy": ".example.com"}, "http://www.example.com/", None),
        # ... but never a different suffix.
        ({"no_proxy": "example.com"}, "http://notexample.com/", _PROXY_URL),
        ({"no_proxy": "example.com"}, "http://example.com.evil.org/", _PROXY_URL),
        # Trailing dots are normalized away on both sides.
        ({"no_proxy": "example.com."}, "http://example.com/", None),
        ({"no_proxy": "example.com"}, "http://example.com./", None),
        # Matching is case-insensitive.
        ({"no_proxy": "EXAMPLE.com"}, "http://WWW.EXAMPLE.COM/", None),
        # Port-qualified tokens only match that port; URLs without an
        # explicit port match against the scheme default.
        ({"no_proxy": "example.com:8080"}, "http://example.com:8080/", None),
        ({"no_proxy": "example.com:8080"}, "http://example.com/", _PROXY_URL),
        ({"no_proxy": "example.com:80"}, "http://example.com/", None),
        ({"no_proxy": "[::1]:8080"}, "http://[::1]:8080/", None),
        ({"no_proxy": "[::1]:8080"}, "http://[::1]:9090/", _PROXY_URL),
        ({"no_proxy": "10.0.0.0/8:8080"}, "http://10.1.2.3:8080/", None),
        ({"no_proxy": "10.0.0.0/8:8080"}, "http://10.1.2.3:9090/", _PROXY_URL),
        # Scheme-qualified tokens only apply to that scheme.
        ({"no_proxy": "http://example.com"}, "http://example.com/", None),
        ({"no_proxy": "http://example.com"}, "https://example.com/", _PROXY_URL),
        # A bare star bypasses everything.
        ({"no_proxy": "*"}, "http://example.com/", None),
        # Token lists are comma-separated and tolerate whitespace.
        ({"no_proxy": "other.org, example.com"}, "http://example.com/", None),
    ),
    ids=lambda v: v if isinstance(v, str) else None,
)
async def test_proxy_routing_decision_table(
    routing: Callable[..., _RoutingHarness],
    proxy_env: dict[str, str],
    env: dict[str, str],
    url: str,
    expected_proxy: str | None,
) -> None:
    proxy_env.update(env)
    if expected_proxy is not None or "no_proxy" in env:
        proxy_env.setdefault("http_proxy", _PROXY_URL)
        proxy_env.setdefault("https_proxy", _PROXY_URL)
    harness = routing(trust_env=True)

    await harness.session.get(url)

    harness.assert_routing(0, expected_proxy)


async def test_env_proxy_auth_is_attached(
    routing: Callable[..., _RoutingHarness], proxy_env: dict[str, str]
) -> None:
    proxy_env["http_proxy"] = "http://user:pass@proxy.example:8080"
    harness = routing(trust_env=True)

    await harness.session.get("http://example.com/")

    harness.assert_routing(0, "http://proxy.example:8080", _PROXY_AUTH)


async def test_env_proxy_auth_not_sent_when_bypassed(
    routing: Callable[..., _RoutingHarness], proxy_env: dict[str, str]
) -> None:
    proxy_env["http_proxy"] = "http://user:pass@proxy.example:8080"
    proxy_env["no_proxy"] = "example.com"
    harness = routing(trust_env=True)

    await harness.session.get("http://example.com/")

    harness.assert_routing(0, None)


async def test_env_snapshot_taken_at_session_creation(
    routing: Callable[..., _RoutingHarness], proxy_env: dict[str, str]
) -> None:
    proxy_env["http_proxy"] = "http://proxy-a.example"
    harness = routing(trust_env=True)
    # Environment changes after session creation must not affect routing.
    proxy_env["http_proxy"] = "http://proxy-b.example"
    proxy_env["no_proxy"] = "example.com"

    await harness.session.get("http://example.com/")

    harness.assert_routing(0, "http://proxy-a.example")


async def test_sessions_do_not_share_proxy_state(
    routing: Callable[..., _RoutingHarness], proxy_env: dict[str, str]
) -> None:
    proxy_env["http_proxy"] = "http://proxy-a.example"
    first = routing(trust_env=True)
    proxy_env["http_proxy"] = "http://proxy-b.example"
    second = routing(trust_env=True)

    await first.session.get("http://example.com/")
    await second.session.get("http://example.com/")

    first.assert_routing(0, "http://proxy-a.example")
    second.assert_routing(0, "http://proxy-b.example")
    # The snapshot is immutable: no session can mutate another's map.
    assert isinstance(first.session._env_proxy_settings.proxies, MappingProxyType)
    with pytest.raises(TypeError):
        first.session._env_proxy_settings.proxies["http"] = (  # type: ignore[index]
            second.session._env_proxy_settings.proxies["http"]
        )


async def test_per_request_proxy_overrides_env_without_leaking_auth(
    routing: Callable[..., _RoutingHarness], proxy_env: dict[str, str]
) -> None:
    proxy_env["http_proxy"] = "http://user:pass@proxy.example:8080"
    harness = routing(trust_env=True)

    await harness.session.get("http://example.com/", proxy="http://other.proxy:3128")

    # The env proxy and its credentials must not leak into the override.
    harness.assert_routing(0, "http://other.proxy:3128")


async def test_session_default_proxy_overrides_env(
    routing: Callable[..., _RoutingHarness], proxy_env: dict[str, str]
) -> None:
    proxy_env["http_proxy"] = _PROXY_URL
    harness = routing(trust_env=True, proxy="http://default.proxy:3128")

    await harness.session.get("http://example.com/")

    harness.assert_routing(0, "http://default.proxy:3128")


async def test_env_ignored_without_trust_env(
    routing: Callable[..., _RoutingHarness], proxy_env: dict[str, str]
) -> None:
    proxy_env["http_proxy"] = _PROXY_URL
    harness = routing(trust_env=False)

    await harness.session.get("http://example.com/")

    harness.assert_routing(0, None)


async def test_unix_connector_bypasses_env_proxy(
    routing: Callable[..., _RoutingHarness], proxy_env: dict[str, str]
) -> None:
    proxy_env["http_proxy"] = _PROXY_URL
    harness = routing(connector=UnixConnector(path="/tmp/socket"), trust_env=True)

    await harness.session.get("http://example.com/")

    harness.assert_routing(0, None)


async def test_redirect_to_bypassed_host_drops_proxy_and_auth(
    routing: Callable[..., _RoutingHarness], proxy_env: dict[str, str]
) -> None:
    proxy_env["http_proxy"] = "http://user:pass@proxy.example:8080"
    proxy_env["no_proxy"] = "bypassed.example"
    harness = routing(
        responses=[
            _make_response(
                302,
                {"Location": "http://bypassed.example/"},
                url="http://proxied.example/",
            ),
            _make_response(url="http://bypassed.example/"),
        ],
        trust_env=True,
    )

    await harness.session.get("http://proxied.example/")

    assert len(harness.recorded) == 2
    harness.assert_routing(0, "http://proxy.example:8080", _PROXY_AUTH)
    # The bypassed redirect target goes direct, without the proxy auth.
    harness.assert_routing(1, None)


async def test_redirect_to_other_proxy_does_not_leak_auth(
    routing: Callable[..., _RoutingHarness], proxy_env: dict[str, str]
) -> None:
    proxy_env["http_proxy"] = "http://user:pass@proxy-a.example"
    proxy_env["https_proxy"] = "http://proxy-b.example"
    harness = routing(
        responses=[
            _make_response(
                302,
                {"Location": "https://other.example/"},
                url="http://proxied.example/",
            ),
            _make_response(url="https://other.example/"),
        ],
        trust_env=True,
    )

    await harness.session.get("http://proxied.example/")

    assert len(harness.recorded) == 2
    harness.assert_routing(0, "http://proxy-a.example", _PROXY_AUTH)
    # The second hop uses the https proxy, without proxy A's credentials.
    harness.assert_routing(1, "http://proxy-b.example")


async def test_explicit_proxy_is_used_for_all_redirect_hops(
    routing: Callable[..., _RoutingHarness], proxy_env: dict[str, str]
) -> None:
    proxy_env["http_proxy"] = _PROXY_URL
    proxy_env["no_proxy"] = "other.example"
    harness = routing(
        responses=[
            _make_response(
                302,
                {"Location": "http://other.example/"},
                url="http://proxied.example/",
            ),
            _make_response(url="http://other.example/"),
        ],
        trust_env=True,
    )

    await harness.session.get(
        "http://proxied.example/", proxy="http://explicit.proxy:3128"
    )

    assert len(harness.recorded) == 2
    # An explicit per-request proxy wins over the env on every hop.
    harness.assert_routing(0, "http://explicit.proxy:3128")
    harness.assert_routing(1, "http://explicit.proxy:3128")
