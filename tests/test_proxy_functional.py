import asyncio
import os
import pathlib
import platform
import socket
import ssl
import sys
from collections.abc import Awaitable, Callable, Iterator
from contextlib import ExitStack, suppress
from re import match as match_regex
from typing import TYPE_CHECKING, TypedDict
from unittest import mock
from uuid import uuid4

import pytest
from multidict import CIMultiDict
from pytest_aiohttp import AiohttpRawServer, AiohttpServer
from pytest_mock import MockerFixture
from yarl import URL

import aiohttp
from aiohttp import ClientResponse, web
from aiohttp.abc import AbstractResolver, ResolveResult
from aiohttp.client import _RequestOptions
from aiohttp.client_exceptions import ClientConnectionError
from aiohttp.test_utils import TestServer

if TYPE_CHECKING:
    import proxy
else:
    proxy = pytest.importorskip("proxy")

ASYNCIO_SUPPORTS_TLS_IN_TLS = sys.version_info >= (3, 11)


class _ResponseArgs(TypedDict):
    status: int
    headers: dict[str, str] | None
    body: bytes | None


if sys.version_info >= (3, 11) and TYPE_CHECKING:
    from typing import Unpack

    async def get_request(
        method: str = "GET",
        *,
        url: str | URL,
        trust_env: bool = False,
        **kwargs: Unpack[_RequestOptions],
    ) -> ClientResponse: ...

else:
    from typing import Any

    async def get_request(
        method: str = "GET",
        *,
        url: str | URL,
        trust_env: bool = False,
        **kwargs: Any,
    ) -> ClientResponse:
        connector = aiohttp.TCPConnector(ssl=False)
        async with aiohttp.ClientSession(
            connector=connector, trust_env=trust_env
        ) as client:
            async with client.request(method, url, **kwargs) as resp:
                return resp


@pytest.fixture
def secure_proxy_url(tls_certificate_pem_path: str) -> Iterator[URL]:
    """Return the URL of an instance of a running secure proxy.

    This fixture also spawns that instance and tears it down after the test.
    """
    proxypy_args = [
        # --threadless does not work on windows, see
        # https://github.com/abhinavsingh/proxy.py/issues/492
        "--threaded" if os.name == "nt" else "--threadless",
        "--num-workers",
        "1",  # the tests only send one query anyway
        "--hostname",
        "127.0.0.1",  # network interface to listen to
        "--port",
        "0",  # ephemeral port, so that kernel allocates a free one
        "--cert-file",
        tls_certificate_pem_path,  # contains both key and cert
        "--key-file",
        tls_certificate_pem_path,  # contains both key and cert
    ]

    with proxy.Proxy(input_args=proxypy_args) as proxy_instance:
        yield URL.build(
            scheme="https",
            host=str(proxy_instance.flags.hostname),
            port=proxy_instance.flags.port,
        )


@pytest.fixture
def web_server_endpoint_payload() -> str:
    return str(uuid4())


@pytest.fixture(params=("http", "https"))
def web_server_endpoint_type(request: pytest.FixtureRequest) -> str:
    return request.param  # type: ignore[no-any-return]


@pytest.fixture
async def web_server_endpoint_url(
    aiohttp_server: AiohttpServer,
    ssl_ctx: ssl.SSLContext,
    web_server_endpoint_payload: str,
    web_server_endpoint_type: str,
) -> URL:
    async def handler(request: web.Request) -> web.Response:
        return web.Response(text=web_server_endpoint_payload)

    app = web.Application()
    app.router.add_route("GET", "/", handler)
    if web_server_endpoint_type == "https":
        server = await aiohttp_server(app, ssl=ssl_ctx)
    else:
        server = await aiohttp_server(app)

    return URL.build(
        scheme=web_server_endpoint_type,
        host=server.host,
        port=server.port,
    )


@pytest.mark.skipif(
    not ASYNCIO_SUPPORTS_TLS_IN_TLS,
    reason="asyncio on this python does not support TLS in TLS",
)
@pytest.mark.parametrize("web_server_endpoint_type", ("http", "https"))
@pytest.mark.filterwarnings(r"ignore:.*ssl.OP_NO_SSL*")
# Filter out the warning from
# https://github.com/abhinavsingh/proxy.py/blob/30574fd0414005dfa8792a6e797023e862bdcf43/proxy/common/utils.py#L226
# otherwise this test will fail because the proxy will die with an error.
async def test_secure_https_proxy_absolute_path(
    client_ssl_ctx: ssl.SSLContext,
    secure_proxy_url: URL,
    web_server_endpoint_url: URL,
    web_server_endpoint_payload: str,
) -> None:
    """Ensure HTTP(S) sites are accessible through a secure proxy."""
    conn = aiohttp.TCPConnector()
    sess = aiohttp.ClientSession(connector=conn)

    async with sess.get(
        web_server_endpoint_url,
        proxy=secure_proxy_url,
        ssl=client_ssl_ctx,  # used for both proxy and endpoint connections
    ) as response:
        assert response.status == 200
        assert await response.text() == web_server_endpoint_payload

    await sess.close()
    await conn.close()
    await asyncio.sleep(0.1)


@pytest.mark.parametrize("web_server_endpoint_type", ("https",))
@pytest.mark.skipif(
    ASYNCIO_SUPPORTS_TLS_IN_TLS, reason="asyncio on this python supports TLS in TLS"
)
@pytest.mark.filterwarnings(r"ignore:.*ssl.OP_NO_SSL*")
# Filter out the warning from
# https://github.com/abhinavsingh/proxy.py/blob/30574fd0414005dfa8792a6e797023e862bdcf43/proxy/common/utils.py#L226
# otherwise this test will fail because the proxy will die with an error.
async def test_https_proxy_unsupported_tls_in_tls(
    client_ssl_ctx: ssl.SSLContext,
    secure_proxy_url: URL,
    web_server_endpoint_type: str,
) -> None:
    """Ensure connecting to TLS endpoints w/ HTTPS proxy needs patching.

    This also checks that a helpful warning on how to patch the env
    is displayed.
    """
    url = URL.build(scheme=web_server_endpoint_type, host="python.org")

    assert url.host is not None
    escaped_host_port = ":".join((url.host.replace(".", r"\."), str(url.port)))
    escaped_proxy_url = str(secure_proxy_url).replace(".", r"\.")

    conn = aiohttp.TCPConnector()
    sess = aiohttp.ClientSession(connector=conn)

    expected_warning_text = (
        r"^"
        r"An HTTPS request is being sent through an HTTPS proxy\. "
        "This support for TLS in TLS is known to be disabled "
        r"in the stdlib asyncio\. This is why you'll probably see "
        r"an error in the log below\.\n\n"
        r"It is possible to enable it via monkeypatching\. "
        r"For more details, see:\n"
        r"\* https://bugs\.python\.org/issue37179\n"
        r"\* https://github\.com/python/cpython/pull/28073\n\n"
        r"You can temporarily patch this as follows:\n"
        r"\* https://docs\.aiohttp\.org/en/stable/client_advanced\.html#proxy-support\n"
        r"\* https://github\.com/aio-libs/aiohttp/discussions/6044\n$"
    )
    type_err = (
        r"transport <asyncio\.sslproto\._SSLProtocolTransport object at "
        r"0x[\d\w]+> is not supported by start_tls\(\)"
    )
    expected_exception_reason = (
        r"^"
        "Cannot initialize a TLS-in-TLS connection to host "
        f"{escaped_host_port!s} through an underlying connection "
        f"to an HTTPS proxy {escaped_proxy_url!s} ssl:{client_ssl_ctx!s} "
        f"[{type_err!s}]"
        r"$"
    )

    with (
        mock.patch.object(aiohttp.connector, "aiofastnet", None),
        pytest.warns(
            RuntimeWarning,
            match=expected_warning_text,
        ),
        pytest.raises(
            ClientConnectionError,
            match=expected_exception_reason,
        ) as conn_err,
    ):
        async with sess.get(url, proxy=secure_proxy_url, ssl=client_ssl_ctx):
            pass

    assert isinstance(conn_err.value.__cause__, TypeError)
    assert match_regex(f"^{type_err!s}$", str(conn_err.value.__cause__))

    await sess.close()
    await conn.close()

    await asyncio.sleep(0.1)


@pytest.mark.skipif(
    platform.system() == "Windows" or sys.implementation.name != "cpython",
    reason="uvloop is not supported on Windows and non-CPython implementations",
)
@pytest.mark.filterwarnings(r"ignore:.*ssl.OP_NO_SSL*")
# Filter out the warning from
# https://github.com/abhinavsingh/proxy.py/blob/30574fd0414005dfa8792a6e797023e862bdcf43/proxy/common/utils.py#L226
# otherwise this test will fail because the proxy will die with an error.
# Parameterizing on use_aiofastnet improves test coverage
@pytest.mark.asyncio(loop_factories=("uvloop",))
@pytest.mark.parametrize("use_aiofastnet", [True, False], ids=["aiofastnet", "native"])
async def test_uvloop_secure_https_proxy(
    use_aiofastnet: bool,
    client_ssl_ctx: ssl.SSLContext,
    ssl_ctx: ssl.SSLContext,
    secure_proxy_url: URL,
) -> None:
    """Ensure HTTPS sites are accessible through a secure proxy without warning when using uvloop."""
    payload = str(uuid4())

    async def handler(request: web.Request) -> web.Response:
        return web.Response(text=payload)

    app = web.Application()
    app.router.add_route("GET", "/", handler)
    server = TestServer(app, host="127.0.0.1")
    await server.start_server(ssl=ssl_ctx)

    url = URL.build(scheme="https", host=server.host, port=server.port)
    conn = aiohttp.TCPConnector(force_close=True)
    sess = aiohttp.ClientSession(connector=conn)
    try:
        with ExitStack() as stack:
            if not use_aiofastnet:
                stack.enter_context(
                    mock.patch.object(aiohttp.connector, "aiofastnet", None)
                )

            async with sess.get(
                url, proxy=secure_proxy_url, ssl=client_ssl_ctx
            ) as response:
                assert response.status == 200
                assert await response.text() == payload
    finally:
        await sess.close()
        await conn.close()
        await server.close()
        await asyncio.sleep(0)
        await asyncio.sleep(0.1)


@pytest.fixture
def proxy_test_server(
    aiohttp_raw_server: AiohttpRawServer, monkeypatch: pytest.MonkeyPatch
) -> Callable[[], Awaitable[mock.Mock]]:
    # Handle all proxy requests and imitate remote server response.

    _patch_ssl_transport(monkeypatch)

    default_response = _ResponseArgs(status=200, headers=None, body=None)

    proxy_mock = mock.Mock()

    async def proxy_handler(request: web.Request) -> web.Response:
        proxy_mock.request = request
        proxy_mock.requests_list.append(request)

        response = default_response.copy()
        if isinstance(proxy_mock.return_value, dict):
            response.update(proxy_mock.return_value)  # type: ignore[typeddict-item]

        headers = response["headers"]
        if not headers:
            headers = {}

        if request.method == "CONNECT":
            response["body"] = None

        response["headers"] = headers

        resp = web.Response(**response)
        await resp.prepare(request)
        await resp.write_eof()
        return resp

    async def proxy_server() -> mock.Mock:
        proxy_mock.request = None
        proxy_mock.auth = None
        proxy_mock.requests_list = []

        server = await aiohttp_raw_server(proxy_handler)

        proxy_mock.server = server
        proxy_mock.url = server.make_url("/")

        return proxy_mock

    return proxy_server


async def test_proxy_http_absolute_path(
    proxy_test_server: Callable[[], Awaitable[mock.Mock]],
) -> None:
    url = "http://aiohttp.io/path?query=yes"
    proxy = await proxy_test_server()

    await get_request(url=url, proxy=proxy.url)

    assert len(proxy.requests_list) == 1
    assert proxy.request.method == "GET"
    assert proxy.request.host == "aiohttp.io"
    assert proxy.request.path_qs == "/path?query=yes"


async def test_proxy_http_raw_path(
    proxy_test_server: Callable[[], Awaitable[mock.Mock]],
) -> None:
    url = "http://aiohttp.io:2561/space sheep?q=can:fly"
    raw_url = "/space%20sheep?q=can:fly"
    proxy = await proxy_test_server()

    await get_request(url=url, proxy=proxy.url)

    assert proxy.request.host == "aiohttp.io"
    assert proxy.request.path_qs == raw_url


async def test_proxy_http_idna_support(
    proxy_test_server: Callable[[], Awaitable[mock.Mock]],
) -> None:
    url = "http://éé.com/"
    proxy = await proxy_test_server()

    await get_request(url=url, proxy=proxy.url)

    assert proxy.request.host == "éé.com"
    assert proxy.request.path_qs == "/"


async def test_proxy_http_connection_error() -> None:
    url = "http://aiohttp.io/path"
    proxy_url = "http://localhost:2242/"

    with pytest.raises(aiohttp.ClientConnectorError):
        await get_request(url=url, proxy=proxy_url)


async def test_proxy_http_bad_response(
    proxy_test_server: Callable[[], Awaitable[mock.Mock]],
) -> None:
    url = "http://aiohttp.io/path"
    proxy = await proxy_test_server()
    proxy.return_value = dict(status=502, headers={"Proxy-Agent": "TestProxy"})

    resp = await get_request(url=url, proxy=proxy.url)

    assert resp.status == 502
    assert resp.headers["Proxy-Agent"] == "TestProxy"


async def test_proxy_http_auth(
    proxy_test_server: Callable[[], Awaitable[mock.Mock]],
) -> None:
    url = "http://aiohttp.io/path"
    proxy = await proxy_test_server()

    await get_request(url=url, proxy=proxy.url)

    assert "Authorization" not in proxy.request.headers
    assert "Proxy-Authorization" not in proxy.request.headers

    auth_header = aiohttp.encode_basic_auth("user", "pass")
    await get_request(url=url, headers={"Authorization": auth_header}, proxy=proxy.url)

    assert "Authorization" in proxy.request.headers
    assert "Proxy-Authorization" not in proxy.request.headers

    await get_request(
        url=url,
        proxy_headers={"Proxy-Authorization": auth_header},
        proxy=proxy.url,
    )

    assert "Authorization" not in proxy.request.headers
    assert "Proxy-Authorization" in proxy.request.headers

    await get_request(
        url=url,
        headers={"Authorization": auth_header},
        proxy_headers={"Proxy-Authorization": auth_header},
        proxy=proxy.url,
    )

    assert "Authorization" in proxy.request.headers
    assert "Proxy-Authorization" in proxy.request.headers


async def test_proxy_http_auth_utf8(
    proxy_test_server: Callable[[], Awaitable[mock.Mock]],
) -> None:
    url = "http://aiohttp.io/path"
    auth_header = aiohttp.encode_basic_auth("юзер", "пасс")
    proxy = await proxy_test_server()

    await get_request(url=url, headers={"Authorization": auth_header}, proxy=proxy.url)

    assert "Authorization" in proxy.request.headers
    assert "Proxy-Authorization" not in proxy.request.headers


async def test_proxy_http_auth_from_url(
    proxy_test_server: Callable[[], Awaitable[mock.Mock]],
) -> None:
    url = "http://aiohttp.io/path"
    proxy = await proxy_test_server()

    auth_url = URL(url).with_user("user").with_password("pass")
    await get_request(url=auth_url, proxy=proxy.url)

    assert "Authorization" in proxy.request.headers
    assert "Proxy-Authorization" not in proxy.request.headers

    proxy_url = URL(proxy.url).with_user("user").with_password("pass")
    await get_request(url=url, proxy=proxy_url)

    assert "Authorization" not in proxy.request.headers
    assert "Proxy-Authorization" in proxy.request.headers


async def test_proxy_http_acquired_cleanup(
    proxy_test_server: Callable[[], Awaitable[mock.Mock]],
) -> None:
    url = "http://aiohttp.io/path"

    conn = aiohttp.TCPConnector()
    sess = aiohttp.ClientSession(connector=conn)
    proxy = await proxy_test_server()

    assert 0 == len(conn._acquired)

    async with sess.get(url, proxy=proxy.url) as resp:
        pass
    assert resp.closed

    assert 0 == len(conn._acquired)

    await sess.close()
    await conn.close()


@pytest.mark.skip("we need to reconsider how we test this")
async def test_proxy_http_acquired_cleanup_force(
    proxy_test_server: Callable[[], Awaitable[mock.Mock]],
) -> None:
    url = "http://aiohttp.io/path"

    conn = aiohttp.TCPConnector(force_close=True)
    sess = aiohttp.ClientSession(connector=conn)
    proxy = await proxy_test_server()

    assert 0 == len(conn._acquired)

    async def request() -> None:
        async with sess.get(url, proxy=proxy.url):
            assert 1 == len(conn._acquired)

    await request()

    assert 0 == len(conn._acquired)

    await sess.close()
    await conn.close()


@pytest.mark.skip("we need to reconsider how we test this")
async def test_proxy_http_multi_conn_limit(
    proxy_test_server: Callable[[], Awaitable[mock.Mock]],
) -> None:
    url = "http://aiohttp.io/path"
    limit, multi_conn_num = 1, 5

    conn = aiohttp.TCPConnector(limit=limit)
    sess = aiohttp.ClientSession(connector=conn)
    proxy = await proxy_test_server()

    current_pid = None

    async def request(pid: int) -> ClientResponse:
        # process requests only one by one
        nonlocal current_pid

        async with sess.get(url, proxy=proxy.url) as resp:
            current_pid = pid
            await asyncio.sleep(0.2)
            assert current_pid == pid

        return resp

    requests = [request(pid) for pid in range(multi_conn_num)]
    responses = await asyncio.gather(*requests)

    assert len(responses) == multi_conn_num
    assert {resp.status for resp in responses} == {200}

    await sess.close()
    await conn.close()


@pytest.mark.xfail
async def test_proxy_https_connect(
    proxy_test_server: Callable[[], Awaitable[mock.Mock]],
) -> None:
    proxy = await proxy_test_server()
    url = "https://www.google.com.ua/search?q=aiohttp proxy"

    await get_request(url=url, proxy=proxy.url)

    connect = proxy.requests_list[0]
    assert connect.method == "CONNECT"
    assert connect.path == "www.google.com.ua:443"
    assert connect.host == "www.google.com.ua"

    assert proxy.request.host == "www.google.com.ua"
    assert proxy.request.path_qs == "/search?q=aiohttp+proxy"


@pytest.mark.xfail
async def test_proxy_https_connect_with_port(
    proxy_test_server: Callable[[], Awaitable[mock.Mock]],
) -> None:
    proxy = await proxy_test_server()
    url = "https://secure.aiohttp.io:2242/path"

    await get_request(url=url, proxy=proxy.url)

    connect = proxy.requests_list[0]
    assert connect.method == "CONNECT"
    assert connect.path == "secure.aiohttp.io:2242"
    assert connect.host == "secure.aiohttp.io:2242"

    assert proxy.request.host == "secure.aiohttp.io:2242"
    assert proxy.request.path_qs == "/path"


@pytest.mark.xfail
async def test_proxy_https_send_body(
    proxy_test_server: Callable[[], Awaitable[mock.Mock]],
) -> None:
    sess = aiohttp.ClientSession()
    try:
        proxy = await proxy_test_server()
        proxy.return_value = {"status": 200, "body": b"1" * (2**20)}
        url = "https://www.google.com.ua/search?q=aiohttp proxy"

        async with sess.get(url, proxy=proxy.url) as resp:
            body = await resp.read()

        assert body == b"1" * (2**20)
    finally:
        await sess.close()


@pytest.mark.xfail
async def test_proxy_https_idna_support(
    proxy_test_server: Callable[[], Awaitable[mock.Mock]],
) -> None:
    url = "https://éé.com/"
    proxy = await proxy_test_server()

    await get_request(url=url, proxy=proxy.url)

    connect = proxy.requests_list[0]
    assert connect.method == "CONNECT"
    assert connect.path == "xn--9caa.com:443"
    assert connect.host == "xn--9caa.com"


async def test_proxy_https_connection_error() -> None:
    url = "https://secure.aiohttp.io/path"
    proxy_url = "http://localhost:2242/"

    with pytest.raises(aiohttp.ClientConnectorError):
        await get_request(url=url, proxy=proxy_url)


async def test_proxy_https_bad_response(
    proxy_test_server: Callable[[], Awaitable[mock.Mock]],
) -> None:
    url = "https://secure.aiohttp.io/path"
    proxy = await proxy_test_server()
    proxy.return_value = dict(status=502, headers={"Proxy-Agent": "TestProxy"})

    with pytest.raises(aiohttp.ClientHttpProxyError):
        await get_request(url=url, proxy=proxy.url)

    assert len(proxy.requests_list) == 1
    assert proxy.request.method == "CONNECT"
    # The following check fails on MacOS
    # assert proxy.request.path == 'secure.aiohttp.io:443'


@pytest.mark.xfail
async def test_proxy_https_auth(
    proxy_test_server: Callable[[], Awaitable[mock.Mock]],
) -> None:
    url = "https://secure.aiohttp.io/path"
    auth_header = aiohttp.encode_basic_auth("user", "pass")

    proxy = await proxy_test_server()
    await get_request(url=url, proxy=proxy.url)

    connect = proxy.requests_list[0]
    assert "Authorization" not in connect.headers
    assert "Proxy-Authorization" not in connect.headers
    assert "Authorization" not in proxy.request.headers
    assert "Proxy-Authorization" not in proxy.request.headers

    proxy = await proxy_test_server()
    await get_request(url=url, headers={"Authorization": auth_header}, proxy=proxy.url)

    connect = proxy.requests_list[0]
    assert "Authorization" not in connect.headers
    assert "Proxy-Authorization" not in connect.headers
    assert "Authorization" in proxy.request.headers
    assert "Proxy-Authorization" not in proxy.request.headers

    proxy = await proxy_test_server()
    await get_request(
        url=url,
        proxy_headers={"Proxy-Authorization": auth_header},
        proxy=proxy.url,
    )

    connect = proxy.requests_list[0]
    assert "Authorization" not in connect.headers
    assert "Proxy-Authorization" in connect.headers
    assert "Authorization" not in proxy.request.headers
    assert "Proxy-Authorization" not in proxy.request.headers

    proxy = await proxy_test_server()
    await get_request(
        url=url,
        headers={"Authorization": auth_header},
        proxy_headers={"Proxy-Authorization": auth_header},
        proxy=proxy.url,
    )

    connect = proxy.requests_list[0]
    assert "Authorization" not in connect.headers
    assert "Proxy-Authorization" in connect.headers
    assert "Authorization" in proxy.request.headers
    assert "Proxy-Authorization" not in proxy.request.headers


@pytest.mark.xfail
async def test_proxy_https_acquired_cleanup(
    proxy_test_server: Callable[[], Awaitable[mock.Mock]],
) -> None:
    url = "https://secure.aiohttp.io/path"

    conn = aiohttp.TCPConnector()
    sess = aiohttp.ClientSession(connector=conn)
    try:
        proxy = await proxy_test_server()

        assert 0 == len(conn._acquired)

        async def request() -> None:
            async with sess.get(url, proxy=proxy.url):
                assert 1 == len(conn._acquired)

        await request()

        assert 0 == len(conn._acquired)
    finally:
        await sess.close()
        await conn.close()


@pytest.mark.xfail
async def test_proxy_https_acquired_cleanup_force(
    proxy_test_server: Callable[[], Awaitable[mock.Mock]],
) -> None:
    url = "https://secure.aiohttp.io/path"

    conn = aiohttp.TCPConnector(force_close=True)
    sess = aiohttp.ClientSession(connector=conn)
    try:
        proxy = await proxy_test_server()

        assert 0 == len(conn._acquired)

        async def request() -> None:
            async with sess.get(url, proxy=proxy.url):
                assert 1 == len(conn._acquired)

        await request()

        assert 0 == len(conn._acquired)
    finally:
        await sess.close()
        await conn.close()


@pytest.mark.xfail
async def test_proxy_https_multi_conn_limit(
    proxy_test_server: Callable[[], Awaitable[mock.Mock]],
) -> None:
    url = "https://secure.aiohttp.io/path"
    limit, multi_conn_num = 1, 5

    conn = aiohttp.TCPConnector(limit=limit)
    sess = aiohttp.ClientSession(connector=conn)
    proxy = await proxy_test_server()

    try:
        current_pid = None

        async def request(pid: int) -> ClientResponse:
            # process requests only one by one
            nonlocal current_pid

            async with sess.get(url, proxy=proxy.url) as resp:
                current_pid = pid
                await asyncio.sleep(0.2)
                assert current_pid == pid

            return resp

        requests = [request(pid) for pid in range(multi_conn_num)]
        responses = await asyncio.gather(*requests, return_exceptions=True)

        # Filter out exceptions to count actual responses
        actual_responses = [r for r in responses if isinstance(r, ClientResponse)]
        assert len(actual_responses) == multi_conn_num
        assert {resp.status for resp in actual_responses} == {200}
    finally:
        await sess.close()
        await conn.close()


def _patch_ssl_transport(monkeypatch: pytest.MonkeyPatch) -> None:
    # Make ssl transport substitution to prevent ssl handshake.
    def _make_ssl_transport_dummy(
        self: asyncio.selector_events.BaseSelectorEventLoop,
        rawsock: object,
        protocol: object,
        sslcontext: object,
        waiter: object = None,
        **kwargs: object,
    ) -> object:
        return self._make_socket_transport(  # type: ignore[attr-defined]
            rawsock,
            protocol,
            waiter,
            extra=kwargs.get("extra"),
            server=kwargs.get("server"),
        )

    monkeypatch.setattr(
        "asyncio.selector_events.BaseSelectorEventLoop._make_ssl_transport",
        _make_ssl_transport_dummy,
    )


original_is_file = pathlib.Path.is_file


def mock_is_file(self: pathlib.Path) -> bool:
    # make real netrc file invisible in home dir
    if self.name in ["_netrc", ".netrc"] and self.parent == self.home():
        return False
    else:
        return original_is_file(self)


async def test_proxy_from_env_http(
    proxy_test_server: Callable[[], Awaitable[mock.Mock]], mocker: MockerFixture
) -> None:
    url = "http://aiohttp.io/path"
    proxy = await proxy_test_server()
    mocker.patch.dict(os.environ, {"http_proxy": str(proxy.url)})
    mocker.patch("pathlib.Path.is_file", mock_is_file)

    await get_request(url=url, trust_env=True)

    assert len(proxy.requests_list) == 1
    assert proxy.request.method == "GET"
    assert proxy.request.host == "aiohttp.io"
    assert proxy.request.path_qs == "/path"
    assert "Proxy-Authorization" not in proxy.request.headers


async def test_proxy_from_env_http_with_auth(
    proxy_test_server: Callable[[], Awaitable[mock.Mock]], mocker: MockerFixture
) -> None:
    url = "http://aiohttp.io/path"
    proxy = await proxy_test_server()
    expected_header = aiohttp.encode_basic_auth("user", "pass")
    mocker.patch.dict(
        os.environ,
        {"http_proxy": str(proxy.url.with_user("user").with_password("pass"))},
    )

    await get_request(url=url, trust_env=True)

    assert len(proxy.requests_list) == 1
    assert proxy.request.method == "GET"
    assert proxy.request.host == "aiohttp.io"
    assert proxy.request.path_qs == "/path"
    assert proxy.request.headers["Proxy-Authorization"] == expected_header


async def test_proxy_from_env_http_with_auth_from_netrc(
    proxy_test_server: Callable[[], Awaitable[mock.Mock]],
    tmp_path: pathlib.Path,
    mocker: MockerFixture,
) -> None:
    url = "http://aiohttp.io/path"
    proxy = await proxy_test_server()
    expected_header = aiohttp.encode_basic_auth("user", "pass")
    netrc_file = tmp_path / "test_netrc"
    netrc_file.write_text("machine 127.0.0.1 login user password pass")
    mocker.patch.dict(
        os.environ, {"http_proxy": str(proxy.url), "NETRC": str(netrc_file)}
    )

    await get_request(url=url, trust_env=True)

    assert len(proxy.requests_list) == 1
    assert proxy.request.method == "GET"
    assert proxy.request.host == "aiohttp.io"
    assert proxy.request.path_qs == "/path"
    assert proxy.request.headers["Proxy-Authorization"] == expected_header


async def test_proxy_from_env_http_without_auth_from_netrc(
    proxy_test_server: Callable[[], Awaitable[mock.Mock]],
    tmp_path: pathlib.Path,
    mocker: MockerFixture,
) -> None:
    url = "http://aiohttp.io/path"
    proxy = await proxy_test_server()
    netrc_file = tmp_path / "test_netrc"
    netrc_file.write_text("machine 127.0.0.2 login user password pass")
    mocker.patch.dict(
        os.environ, {"http_proxy": str(proxy.url), "NETRC": str(netrc_file)}
    )

    await get_request(url=url, trust_env=True)

    assert len(proxy.requests_list) == 1
    assert proxy.request.method == "GET"
    assert proxy.request.host == "aiohttp.io"
    assert proxy.request.path_qs == "/path"
    assert "Proxy-Authorization" not in proxy.request.headers


async def test_proxy_from_env_http_without_auth_from_wrong_netrc(
    proxy_test_server: Callable[[], Awaitable[mock.Mock]],
    tmp_path: pathlib.Path,
    mocker: MockerFixture,
) -> None:
    url = "http://aiohttp.io/path"
    proxy = await proxy_test_server()
    netrc_file = tmp_path / "test_netrc"
    netrc_file.write_text("machine 127.0.0.1 user pass pass")
    mocker.patch.dict(
        os.environ, {"http_proxy": str(proxy.url), "NETRC": str(netrc_file)}
    )

    await get_request(url=url, trust_env=True)

    assert len(proxy.requests_list) == 1
    assert proxy.request.method == "GET"
    assert proxy.request.host == "aiohttp.io"
    assert proxy.request.path_qs == "/path"
    assert "Proxy-Authorization" not in proxy.request.headers


async def test_proxy_from_env_auth_scoped_to_redirect_selected_proxy(
    aiohttp_raw_server: AiohttpRawServer,
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: pathlib.Path,
) -> None:
    # Redirect from an authenticated http_proxy to an HTTPS target served by a
    # separate https_proxy must not leak the first proxy's Proxy-Authorization
    # onto the second proxy's CONNECT request.
    auth_header = aiohttp.encode_basic_auth("user", "pass")
    http_proxy_requests: list[CIMultiDict[str]] = []
    https_proxy_requests: list[CIMultiDict[str]] = []

    async def http_proxy_handler(request: web.Request) -> web.Response:
        http_proxy_requests.append(CIMultiDict(request.headers))
        return web.Response(
            status=302, headers={"Location": "https://attacker.example/secret"}
        )

    async def https_proxy_handler(request: web.Request) -> web.Response:
        https_proxy_requests.append(CIMultiDict(request.headers))
        return web.Response(status=502)

    http_proxy = await aiohttp_raw_server(http_proxy_handler)
    https_proxy = await aiohttp_raw_server(https_proxy_handler)

    for name in (
        "http_proxy",
        "https_proxy",
        "all_proxy",
        "no_proxy",
        "HTTP_PROXY",
        "HTTPS_PROXY",
        "ALL_PROXY",
        "NO_PROXY",
    ):
        monkeypatch.delenv(name, raising=False)
    netrc_file = tmp_path / "empty_netrc"
    netrc_file.write_text("")
    http_proxy_url = http_proxy.make_url("/").with_user("user").with_password("pass")
    monkeypatch.setenv("http_proxy", str(http_proxy_url))
    monkeypatch.setenv("https_proxy", str(https_proxy.make_url("/")))
    monkeypatch.setenv("NETRC", str(netrc_file))

    with pytest.raises(aiohttp.ClientHttpProxyError):
        await get_request(url="http://victim.example/redirect", trust_env=True)

    assert len(http_proxy_requests) == 1
    assert http_proxy_requests[0]["Proxy-Authorization"] == auth_header
    assert len(https_proxy_requests) == 1
    assert "Proxy-Authorization" not in https_proxy_requests[0]


@pytest.mark.xfail
async def test_proxy_from_env_https(
    proxy_test_server: Callable[[], Awaitable[mock.Mock]], mocker: MockerFixture
) -> None:
    url = "https://aiohttp.io/path"
    proxy = await proxy_test_server()
    mocker.patch.dict(os.environ, {"https_proxy": str(proxy.url)})
    mocker.patch("pathlib.Path.is_file", mock_is_file)

    await get_request(url=url, trust_env=True)

    assert len(proxy.requests_list) == 2
    assert proxy.request.method == "GET"
    assert proxy.request.host == "aiohttp.io"
    assert proxy.request.path_qs == "/path"
    assert "Proxy-Authorization" not in proxy.request.headers


@pytest.mark.xfail
async def test_proxy_from_env_https_with_auth(
    proxy_test_server: Callable[[], Awaitable[mock.Mock]], mocker: MockerFixture
) -> None:
    url = "https://aiohttp.io/path"
    proxy = await proxy_test_server()
    expected_header = aiohttp.encode_basic_auth("user", "pass")
    mocker.patch.dict(
        os.environ,
        {"https_proxy": str(proxy.url.with_user("user").with_password("pass"))},
    )

    await get_request(url=url, trust_env=True)

    assert len(proxy.requests_list) == 2

    assert proxy.request.method == "GET"
    assert proxy.request.host == "aiohttp.io"
    assert proxy.request.path_qs == "/path"
    assert "Proxy-Authorization" not in proxy.request.headers

    r2 = proxy.requests_list[0]
    assert r2.method == "CONNECT"
    assert r2.host == "aiohttp.io"
    assert r2.path_qs == "/path"
    assert r2.headers["Proxy-Authorization"] == expected_header


async def test_https_proxy_connect_tunnel_session_close_no_hang(
    aiohttp_server: AiohttpServer,
) -> None:
    """Test that CONNECT tunnel connections are not pooled."""
    # Regression test for issue #11273.

    # Create a minimal proxy server
    # The CONNECT method is handled at the protocol level, not by the handler
    proxy_app = web.Application()
    proxy_server = await aiohttp_server(proxy_app)
    proxy_url = f"http://{proxy_server.host}:{proxy_server.port}"

    # Create session and make HTTPS request through proxy
    session = aiohttp.ClientSession()

    try:
        # This will fail during TLS upgrade because proxy doesn't establish tunnel
        with suppress(aiohttp.ClientError):
            async with session.get("https://example.com/test", proxy=proxy_url) as resp:
                await resp.read()

        # The critical test: Check if any connections were pooled with proxy=None
        # This is the root cause of the hang - CONNECT tunnel connections
        # should NOT be pooled
        connector = session.connector
        assert connector is not None

        # Count connections with proxy=None in the pool
        proxy_none_keys = [key for key in connector._conns if key.proxy is None]
        proxy_none_count = len(proxy_none_keys)

        # Before the fix, there would be a connection with proxy=None
        # After the fix, CONNECT tunnel connections are not pooled
        assert proxy_none_count == 0, (
            f"Found {proxy_none_count} connections with proxy=None in pool. "
            f"CONNECT tunnel connections should not be pooled - this is bug #11273"
        )

    finally:
        # Clean close
        await session.close()


class _RecordingResolver(AbstractResolver):
    """Fake resolver that maps every host to 127.0.0.1 and records lookups."""

    def __init__(self) -> None:
        self.queries: list[tuple[str, int]] = []

    async def resolve(
        self, host: str, port: int = 0, family: int = socket.AF_INET
    ) -> list[ResolveResult]:
        self.queries.append((host, port))
        return [
            {
                "hostname": host,
                "host": "127.0.0.1",
                "port": port,
                "family": family,
                "proto": 0,
                "flags": socket.AI_NUMERIC,
            }
        ]

    async def close(self) -> None:
        pass


def _clear_proxy_env(monkeypatch: pytest.MonkeyPatch) -> None:
    for name in (
        "http_proxy",
        "https_proxy",
        "ws_proxy",
        "wss_proxy",
        "all_proxy",
        "no_proxy",
    ):
        for env_name in (name, name.upper()):
            monkeypatch.delenv(env_name, raising=False)


async def _start_recording_proxy(
    aiohttp_raw_server: AiohttpRawServer, requests: list[web.Request]
) -> URL:
    """Start a fake proxy that records incoming requests and answers 200."""

    async def handler(request: web.Request) -> web.Response:
        requests.append(request)
        return web.Response(status=200)

    server = await aiohttp_raw_server(handler)
    return server.make_url("/")


@pytest.mark.parametrize(
    ("no_proxy", "url_template", "expect_direct"),
    (
        # Domain tokens: exact, leading dot, subdomain and trailing dot.
        ("", "http://routing.example:{port}/path", False),
        ("routing.example", "http://routing.example:{port}/path", True),
        (".routing.example", "http://routing.example:{port}/path", True),
        ("routing.example.", "http://routing.example:{port}/path", True),
        ("routing.example", "http://www.routing.example:{port}/path", True),
        ("www.routing.example", "http://routing.example:{port}/path", False),
        ("notrouting.example", "http://routing.example:{port}/path", False),
        # Port tokens, matched against the explicit target port.
        ("routing.example:{port}", "http://routing.example:{port}/path", True),
        ("routing.example:1", "http://routing.example:{port}/path", False),
        # IPv4 literal and CIDR tokens.
        ("127.0.0.1", "http://127.0.0.1:{port}/path", True),
        ("127.0.0.0/8", "http://127.0.0.1:{port}/path", True),
        ("10.0.0.0/8", "http://127.0.0.1:{port}/path", False),
        # A bare asterisk bypasses everything.
        ("*", "http://routing.example:{port}/path", True),
    ),
)
async def test_proxy_from_env_routing_decision_table(
    no_proxy: str,
    url_template: str,
    expect_direct: bool,
    proxy_test_server: Callable[[], Awaitable[mock.Mock]],
    aiohttp_raw_server: AiohttpRawServer,
    monkeypatch: pytest.MonkeyPatch,
    mocker: MockerFixture,
) -> None:
    """Every NO_PROXY token kind routes to either a direct or a proxied hop."""
    proxy = await proxy_test_server()
    mocker.patch("pathlib.Path.is_file", mock_is_file)

    origin_requests: list[web.Request] = []

    async def origin_handler(request: web.Request) -> web.Response:
        origin_requests.append(request)
        return web.Response(text="direct")

    origin = await aiohttp_raw_server(origin_handler)
    url = url_template.format(port=origin.port)

    resolver = _RecordingResolver()
    connector = aiohttp.TCPConnector(resolver=resolver)
    _clear_proxy_env(monkeypatch)
    monkeypatch.setenv("http_proxy", str(proxy.url))
    monkeypatch.setenv("no_proxy", no_proxy.format(port=origin.port))

    async with aiohttp.ClientSession(connector=connector, trust_env=True) as session:
        async with session.get(url) as resp:
            assert resp.status == 200
            await resp.read()

    if expect_direct:
        # The origin server was contacted directly...
        assert len(origin_requests) == 1
        # ...and the recording proxy saw nothing.
        assert proxy.requests_list == []
        # Hostname targets were resolved by the client resolver;
        # IP literal targets need no resolution at all.
        host = URL(url).host
        assert host is not None
        if not match_regex(r"^[0-9.]+$", host):
            assert (host, origin.port) in resolver.queries
    else:
        # The request went through the proxy in absolute form...
        assert len(proxy.requests_list) == 1
        target_host = URL(url).host
        assert target_host is not None
        assert proxy.requests_list[0].host == target_host
        assert proxy.requests_list[0].path_qs == "/path"
        # ...and the origin server was never contacted directly.
        assert origin_requests == []
        # The client never resolved the target host itself.
        assert resolver.queries == []


@pytest.mark.parametrize(
    ("no_proxy", "expect_direct"),
    (
        ("", False),
        ("routing.example", True),
        # A port token matching the explicit CONNECT port bypasses too.
        ("routing.example:1", True),
        ("routing.example:2", False),
    ),
)
async def test_proxy_from_env_https_connect_decision(
    no_proxy: str,
    expect_direct: bool,
    proxy_test_server: Callable[[], Awaitable[mock.Mock]],
    monkeypatch: pytest.MonkeyPatch,
    mocker: MockerFixture,
) -> None:
    """HTTPS targets either open a CONNECT tunnel or go direct."""
    proxy = await proxy_test_server()
    mocker.patch("pathlib.Path.is_file", mock_is_file)

    resolver = _RecordingResolver()
    connector = aiohttp.TCPConnector(resolver=resolver, ssl=False)
    _clear_proxy_env(monkeypatch)
    monkeypatch.setenv("https_proxy", str(proxy.url))
    monkeypatch.setenv("no_proxy", no_proxy)

    # Port 1 is never listening, so a direct connection always fails fast.
    url = "https://routing.example:1/"
    async with aiohttp.ClientSession(connector=connector, trust_env=True) as session:
        if expect_direct:
            with pytest.raises(ClientConnectionError):
                await session.get(url)
            # The proxy was not involved at all.
            assert proxy.requests_list == []
            assert ("routing.example", 1) in resolver.queries
        else:
            with suppress(aiohttp.ClientError):
                await session.get(url)
            # The first recorded request is the CONNECT tunnel request.
            assert len(proxy.requests_list) >= 1
            connect_request = proxy.requests_list[0]
            assert connect_request.method == "CONNECT"
            assert connect_request.raw_path == "routing.example:1"
            assert resolver.queries == []


async def test_proxy_from_env_snapshot_ignores_later_env_changes(
    aiohttp_raw_server: AiohttpRawServer,
    monkeypatch: pytest.MonkeyPatch,
    mocker: MockerFixture,
) -> None:
    """A session keeps using the env snapshot taken at its creation."""
    mocker.patch("pathlib.Path.is_file", mock_is_file)
    proxy_one_requests: list[web.Request] = []
    proxy_two_requests: list[web.Request] = []
    proxy_one_url = await _start_recording_proxy(aiohttp_raw_server, proxy_one_requests)
    proxy_two_url = await _start_recording_proxy(aiohttp_raw_server, proxy_two_requests)

    _clear_proxy_env(monkeypatch)
    monkeypatch.setenv("http_proxy", str(proxy_one_url))
    async with aiohttp.ClientSession(trust_env=True) as session:
        # The environment changes after the session was created...
        monkeypatch.setenv("http_proxy", str(proxy_two_url))
        async with session.get("http://routing.example/") as resp:
            assert resp.status == 200
        # ...but the session still routes via the first proxy.
        assert len(proxy_one_requests) == 1
        assert proxy_two_requests == []

    # A session created after the change picks up the new environment.
    async with aiohttp.ClientSession(trust_env=True) as session2:
        async with session2.get("http://routing.example/") as resp:
            assert resp.status == 200
    assert len(proxy_one_requests) == 1
    assert len(proxy_two_requests) == 1


async def test_per_request_proxy_overrides_env_without_leaking_auth(
    aiohttp_raw_server: AiohttpRawServer,
    monkeypatch: pytest.MonkeyPatch,
    mocker: MockerFixture,
) -> None:
    """A per-request proxy wins over the env and gets no env-proxy auth."""
    mocker.patch("pathlib.Path.is_file", mock_is_file)
    env_proxy_requests: list[web.Request] = []
    explicit_proxy_requests: list[web.Request] = []
    env_proxy_url = await _start_recording_proxy(aiohttp_raw_server, env_proxy_requests)
    explicit_proxy_url = await _start_recording_proxy(
        aiohttp_raw_server, explicit_proxy_requests
    )

    _clear_proxy_env(monkeypatch)
    env_proxy_with_auth = (
        URL(str(env_proxy_url)).with_user("user").with_password("pass")
    )
    monkeypatch.setenv("http_proxy", str(env_proxy_with_auth))
    async with aiohttp.ClientSession(trust_env=True) as session:
        async with session.get(
            "http://routing.example/", proxy=str(explicit_proxy_url)
        ) as resp:
            assert resp.status == 200

    # The env proxy was bypassed entirely...
    assert env_proxy_requests == []
    # ...and its credentials were not leaked to the explicit proxy.
    assert len(explicit_proxy_requests) == 1
    assert "Proxy-Authorization" not in explicit_proxy_requests[0].headers


async def test_redirect_reevaluates_no_proxy_per_hop(
    proxy_test_server: Callable[[], Awaitable[mock.Mock]],
    aiohttp_raw_server: AiohttpRawServer,
    monkeypatch: pytest.MonkeyPatch,
    mocker: MockerFixture,
) -> None:
    """A redirect to a NO_PROXY host leaves the proxy for the next hop."""
    proxy = await proxy_test_server()
    mocker.patch("pathlib.Path.is_file", mock_is_file)

    origin_requests: list[web.Request] = []

    async def origin_handler(request: web.Request) -> web.Response:
        origin_requests.append(request)
        return web.Response(text="final")

    origin = await aiohttp_raw_server(origin_handler)
    # The first hop is proxied and redirects to a bypassed host.
    proxy.return_value = {
        "status": 302,
        "headers": {"Location": f"http://bypass.example:{origin.port}/final"},
    }

    resolver = _RecordingResolver()
    connector = aiohttp.TCPConnector(resolver=resolver)
    _clear_proxy_env(monkeypatch)
    monkeypatch.setenv("http_proxy", str(proxy.url))
    monkeypatch.setenv("no_proxy", "bypass.example")

    async with aiohttp.ClientSession(connector=connector, trust_env=True) as session:
        async with session.get("http://routing.example/start") as resp:
            assert resp.status == 200
            assert await resp.text() == "final"

    # The first hop went through the proxy...
    assert len(proxy.requests_list) == 1
    # ...while the redirect target was fetched directly.
    assert len(origin_requests) == 1
    assert ("bypass.example", origin.port) in resolver.queries


async def test_proxy_from_env_not_used_for_unix_socket(
    unix_sockname: str,
    monkeypatch: pytest.MonkeyPatch,
    mocker: MockerFixture,
) -> None:
    """Unix socket connectors always connect directly, ignoring env proxies."""
    mocker.patch("pathlib.Path.is_file", mock_is_file)
    seen_paths: list[str] = []

    async def handler(request: web.Request) -> web.Response:
        seen_paths.append(request.raw_path)
        return web.Response(text="ok")

    app = web.Application()
    app.router.add_get("/path", handler)
    runner = web.AppRunner(app)
    await runner.setup()
    site = web.UnixSite(runner, unix_sockname)
    await site.start()
    try:
        _clear_proxy_env(monkeypatch)
        # If the env proxy were applied, the request would be sent to an
        # unresolvable proxy host in absolute form; it must not be.
        monkeypatch.setenv("http_proxy", "http://proxy.invalid:8080")
        async with aiohttp.ClientSession(
            connector=aiohttp.UnixConnector(unix_sockname), trust_env=True
        ) as session:
            async with session.get("http://localhost/path") as resp:
                assert resp.status == 200
                assert await resp.text() == "ok"
        # The request reached the unix socket in origin form.
        assert seen_paths == ["/path"]
    finally:
        await runner.cleanup()
