import asyncio
import logging
import sys
from collections.abc import AsyncIterator, Awaitable, Callable
from types import SimpleNamespace
from typing import Any, NoReturn
from unittest import mock
from unittest.mock import Mock

import pytest
from aiosignal import Signal
from pytest_aiohttp import AiohttpClient

import aiohttp
from aiohttp import ClientSession, TCPConnector, web
from aiohttp.tracing import (
    Trace,
    TraceConfig,
    TraceConnectionCreateEndParams,
    TraceConnectionCreateStartParams,
    TraceConnectionQueuedEndParams,
    TraceConnectionQueuedStartParams,
    TraceConnectionReuseconnParams,
    TraceDnsCacheHitParams,
    TraceDnsCacheMissParams,
    TraceDnsResolveHostEndParams,
    TraceDnsResolveHostStartParams,
    TraceRequestChunkSentParams,
    TraceRequestEndParams,
    TraceRequestExceptionParams,
    TraceRequestRedirectParams,
    TraceRequestStartParams,
    TraceResponseChunkReceivedParams,
)

if sys.version_info >= (3, 11):
    from typing import assert_type


_SEND_CASES = [
    ("request_start", (Mock(), Mock(), Mock()), TraceRequestStartParams),
    (
        "request_chunk_sent",
        (Mock(), Mock(), Mock()),
        TraceRequestChunkSentParams,
    ),
    (
        "response_chunk_received",
        (Mock(), Mock(), Mock()),
        TraceResponseChunkReceivedParams,
    ),
    ("request_end", (Mock(), Mock(), Mock(), Mock()), TraceRequestEndParams),
    (
        "request_exception",
        (Mock(), Mock(), Mock(), Mock()),
        TraceRequestExceptionParams,
    ),
    (
        "request_redirect",
        (Mock(), Mock(), Mock(), Mock()),
        TraceRequestRedirectParams,
    ),
    ("connection_queued_start", (), TraceConnectionQueuedStartParams),
    ("connection_queued_end", (), TraceConnectionQueuedEndParams),
    ("connection_create_start", (), TraceConnectionCreateStartParams),
    ("connection_create_end", (), TraceConnectionCreateEndParams),
    ("connection_reuseconn", (), TraceConnectionReuseconnParams),
    ("dns_resolvehost_start", (Mock(),), TraceDnsResolveHostStartParams),
    ("dns_resolvehost_end", (Mock(),), TraceDnsResolveHostEndParams),
    ("dns_cache_hit", (Mock(),), TraceDnsCacheHitParams),
    ("dns_cache_miss", (Mock(),), TraceDnsCacheMissParams),
]


class TestTraceConfig:
    def test_trace_config_ctx_default(self) -> None:
        trace_config = TraceConfig()
        assert isinstance(trace_config.trace_config_ctx(), SimpleNamespace)
        if sys.version_info >= (3, 11):
            assert_type(
                trace_config.on_request_chunk_sent,
                Signal[ClientSession, SimpleNamespace, TraceRequestChunkSentParams],
            )

    def test_trace_config_ctx_factory(self) -> None:
        trace_config = TraceConfig(trace_config_ctx_factory=dict)
        assert isinstance(trace_config.trace_config_ctx(), dict)
        if sys.version_info >= (3, 11):
            assert_type(
                trace_config.on_request_start,
                Signal[ClientSession, dict[str, Any], TraceRequestStartParams],
            )

    def test_trace_config_ctx_request_ctx(self) -> None:
        trace_request_ctx = Mock()
        trace_config = TraceConfig()
        trace_config_ctx = trace_config.trace_config_ctx(
            trace_request_ctx=trace_request_ctx
        )
        assert trace_config_ctx.trace_request_ctx is trace_request_ctx

    def test_trace_config_ctx_custom_class(self) -> None:
        """Custom class instances should be accepted as trace_request_ctx (#10753)."""

        class MyContext:
            def __init__(self, request_id: int) -> None:
                self.request_id = request_id

        ctx = MyContext(request_id=42)
        trace_config = TraceConfig()
        trace_config_ctx = trace_config.trace_config_ctx(trace_request_ctx=ctx)
        assert trace_config_ctx.trace_request_ctx is ctx
        assert trace_config_ctx.trace_request_ctx.request_id == 42

    def test_freeze(self) -> None:
        trace_config = TraceConfig()
        trace_config.freeze()

        assert trace_config.on_request_start.frozen
        assert trace_config.on_request_chunk_sent.frozen
        assert trace_config.on_response_chunk_received.frozen
        assert trace_config.on_request_end.frozen
        assert trace_config.on_request_exception.frozen
        assert trace_config.on_request_redirect.frozen
        assert trace_config.on_connection_queued_start.frozen
        assert trace_config.on_connection_queued_end.frozen
        assert trace_config.on_connection_create_start.frozen
        assert trace_config.on_connection_create_end.frozen
        assert trace_config.on_connection_reuseconn.frozen
        assert trace_config.on_dns_resolvehost_start.frozen
        assert trace_config.on_dns_resolvehost_end.frozen
        assert trace_config.on_dns_cache_hit.frozen
        assert trace_config.on_dns_cache_miss.frozen
        assert trace_config.on_request_headers_sent.frozen


class TestTrace:
    @pytest.mark.parametrize(
        "signal,params,param_obj",
        _SEND_CASES,
    )
    async def test_send(  # type: ignore[misc]
        self, signal: str, params: tuple[Mock, ...], param_obj: Any
    ) -> None:
        session = Mock()
        trace_request_ctx = Mock()
        callback = mock.AsyncMock()

        trace_config = TraceConfig()
        getattr(trace_config, "on_%s" % signal).append(callback)
        trace_config.freeze()
        trace = Trace(
            session,
            trace_config,
            trace_config.trace_config_ctx(trace_request_ctx=trace_request_ctx),
        )
        await getattr(trace, "send_%s" % signal)(*params)

        callback.assert_called_once_with(
            session,
            SimpleNamespace(trace_request_ctx=trace_request_ctx),
            param_obj(*params),
        )

    @pytest.mark.parametrize("signal,params,param_obj", _SEND_CASES)
    async def test_send_callback_exception_isolated(  # type: ignore[misc]
        self,
        signal: str,
        params: tuple[Mock, ...],
        param_obj: Any,
        caplog: pytest.LogCaptureFixture,
    ) -> None:
        """An Exception in a trace callback is logged, never propagated."""
        session = Mock()
        callback = mock.AsyncMock(side_effect=RuntimeError("boom"))

        trace_config = TraceConfig()
        getattr(trace_config, "on_%s" % signal).append(callback)
        trace_config.freeze()
        trace = Trace(session, trace_config, trace_config.trace_config_ctx())

        with caplog.at_level(logging.ERROR, logger="aiohttp.client"):
            await getattr(trace, "send_%s" % signal)(*params)

        callback.assert_called_once()
        assert "on_%s" % signal in caplog.text
        assert any(
            record.name == "aiohttp.client" and record.exc_info is not None
            for record in caplog.records
        )

    async def test_send_callback_cancelled_propagates(self) -> None:
        """Cancellation is not swallowed by the isolation boundary."""
        session = Mock()
        callback = mock.AsyncMock(side_effect=asyncio.CancelledError)

        trace_config = TraceConfig()
        trace_config.on_request_start.append(callback)
        trace_config.freeze()
        trace = Trace(session, trace_config, trace_config.trace_config_ctx())

        with pytest.raises(asyncio.CancelledError):
            await trace.send_request_start(Mock(), Mock(), Mock())


TERMINAL_SIGNALS = ("request_end", "request_exception")


class TraceRecorder:
    """Records every signal of one TraceConfig as a replayable event log.

    Events are stored as (signal name, params, trace_config_ctx) tuples so
    tests can assert on ordering, URLs and ctx identity.
    """

    SIGNAL_NAMES = (
        "request_start",
        "request_chunk_sent",
        "response_chunk_received",
        "request_end",
        "request_exception",
        "request_redirect",
        "connection_queued_start",
        "connection_queued_end",
        "connection_create_start",
        "connection_create_end",
        "connection_reuseconn",
        "dns_resolvehost_start",
        "dns_resolvehost_end",
        "dns_cache_hit",
        "dns_cache_miss",
        "request_headers_sent",
    )

    def __init__(self) -> None:
        self.events: list[tuple[str, Any, Any]] = []
        self.trace_config = TraceConfig()
        for name in self.SIGNAL_NAMES:
            signal = getattr(self.trace_config, "on_" + name)
            signal.append(self._make_callback(name))

    def _make_callback(
        self, name: str
    ) -> Callable[[ClientSession, Any, Any], Awaitable[None]]:
        async def callback(session: ClientSession, ctx: Any, params: Any) -> None:
            self.events.append((name, params, ctx))

        return callback

    @property
    def signals(self) -> list[str]:
        return [name for name, _, _ in self.events]

    @property
    def terminals(self) -> list[str]:
        return [name for name in self.signals if name in TERMINAL_SIGNALS]

    def params_of(self, signal: str) -> Any:
        return next(params for name, params, _ in self.events if name == signal)

    def assert_single_ctx(self) -> None:
        assert self.events
        first_ctx = self.events[0][2]
        assert all(ctx is first_ctx for _, _, ctx in self.events)


class _FailingResolver:
    async def resolve(self, host: str, port: int, family: int = 0) -> list[Any]:
        raise OSError(-2, "Name or service not known")

    async def close(self) -> None:
        pass


async def test_lifecycle_success(aiohttp_client: AiohttpClient) -> None:
    recorder = TraceRecorder()

    async def handler(request: web.Request) -> web.Response:
        return web.Response(body=b"ok")

    app = web.Application()
    app.router.add_get("/", handler)
    client = await aiohttp_client(app, trace_configs=[recorder.trace_config])

    async with client.get("/") as resp:
        assert resp.status == 200
        assert await resp.read() == b"ok"

    signals = recorder.signals
    assert signals[0] == "request_start"
    assert recorder.terminals == ["request_end"]
    # The connection attempt span is balanced.
    assert signals.count("connection_create_start") == 1
    assert signals.count("connection_create_end") == 1
    assert signals.index("connection_create_start") < signals.index(
        "connection_create_end"
    )
    # Headers go out after the connection is up, before the terminal signal.
    assert signals.index("connection_create_end") < signals.index(
        "request_headers_sent"
    )
    assert signals.index("request_headers_sent") < signals.index("request_end")
    # The whole lifecycle shares one trace_config_ctx.
    recorder.assert_single_ctx()
    # Every URL-carrying event points at the requested URL.
    urls = {params.url for _, params, _ in recorder.events if hasattr(params, "url")}
    assert urls == {client.make_url("/")}


async def test_lifecycle_redirect(aiohttp_client: AiohttpClient) -> None:
    recorder = TraceRecorder()

    async def handler(request: web.Request) -> web.Response:
        return web.Response(body=b"ok")

    async def redirect(request: web.Request) -> NoReturn:
        raise web.HTTPFound("/")

    app = web.Application()
    app.router.add_get("/", handler)
    app.router.add_get("/redirect", redirect)
    client = await aiohttp_client(app, trace_configs=[recorder.trace_config])

    async with client.get("/redirect") as resp:
        assert resp.status == 200
        await resp.read()

    signals = recorder.signals
    assert recorder.terminals == ["request_end"]
    assert signals.count("request_redirect") == 1
    assert signals.index("request_redirect") < signals.index("request_end")
    # The redirect signal carries the original URL, the end signal the final one.
    assert recorder.params_of("request_redirect").url == client.make_url("/redirect")
    assert recorder.params_of("request_end").url == client.make_url("/")
    # Redirect hops belong to the same logical request: one stable ctx.
    recorder.assert_single_ctx()


@pytest.mark.parametrize("use_dns_cache", [True, False])
async def test_lifecycle_dns_failure(use_dns_cache: bool) -> None:
    recorder = TraceRecorder()
    connector = TCPConnector(resolver=_FailingResolver(), use_dns_cache=use_dns_cache)
    async with ClientSession(
        connector=connector, trace_configs=[recorder.trace_config]
    ) as session:
        with pytest.raises(aiohttp.ClientConnectorError):
            await session.get("http://unresolvable.example/")

    signals = recorder.signals
    assert signals[0] == "request_start"
    assert recorder.terminals == ["request_exception"]
    assert signals[-1] == "request_exception"
    # The DNS span is balanced even though the lookup failed.
    assert signals.count("dns_resolvehost_start") == 1
    assert signals.count("dns_resolvehost_end") == 1
    assert signals.index("dns_resolvehost_start") < signals.index("dns_resolvehost_end")
    # The connection attempt span is balanced too.
    assert signals.count("connection_create_start") == 1
    assert signals.count("connection_create_end") == 1
    recorder.assert_single_ctx()
    # The traced exception is the one raised to the caller.
    assert isinstance(
        recorder.params_of("request_exception").exception,
        aiohttp.ClientConnectorError,
    )


async def test_lifecycle_cancel(aiohttp_client: AiohttpClient) -> None:
    recorder = TraceRecorder()
    started = asyncio.Event()

    async def handler(request: web.Request) -> web.Response:
        started.set()
        await asyncio.sleep(60)
        return web.Response()

    app = web.Application()
    app.router.add_get("/", handler)
    client = await aiohttp_client(app, trace_configs=[recorder.trace_config])

    task = asyncio.ensure_future(client.get("/"))
    await started.wait()
    task.cancel()
    with pytest.raises(asyncio.CancelledError):
        await task

    signals = recorder.signals
    assert signals[0] == "request_start"
    assert recorder.terminals == ["request_exception"]
    assert signals[-1] == "request_exception"
    assert isinstance(
        recorder.params_of("request_exception").exception,
        asyncio.CancelledError,
    )
    recorder.assert_single_ctx()


async def test_lifecycle_timeout(aiohttp_client: AiohttpClient) -> None:
    recorder = TraceRecorder()

    async def handler(request: web.Request) -> web.Response:
        await asyncio.sleep(60)
        return web.Response()

    app = web.Application()
    app.router.add_get("/", handler)
    client = await aiohttp_client(
        app,
        trace_configs=[recorder.trace_config],
        timeout=aiohttp.ClientTimeout(total=0.1),
    )

    with pytest.raises(asyncio.TimeoutError):
        await client.get("/")

    signals = recorder.signals
    assert recorder.terminals == ["request_exception"]
    assert signals[-1] == "request_exception"
    assert isinstance(
        recorder.params_of("request_exception").exception, asyncio.TimeoutError
    )


async def test_lifecycle_upload_stream_exception(aiohttp_client: AiohttpClient) -> None:
    recorder = TraceRecorder()

    async def handler(request: web.Request) -> web.Response:
        await request.read()
        return web.Response()

    app = web.Application()
    app.router.add_post("/", handler)
    client = await aiohttp_client(app, trace_configs=[recorder.trace_config])

    async def body() -> AsyncIterator[bytes]:
        yield b"chunk"
        raise RuntimeError("upload boom")

    with pytest.raises(aiohttp.ClientError):
        await client.post("/", data=body())

    signals = recorder.signals
    assert recorder.terminals == ["request_exception"]
    assert signals[-1] == "request_exception"
    assert "request_end" not in signals
    recorder.assert_single_ctx()


async def test_lifecycle_response_stream_error_after_end(
    aiohttp_client: AiohttpClient,
) -> None:
    recorder = TraceRecorder()

    async def handler(request: web.Request) -> web.StreamResponse:
        resp = web.StreamResponse()
        resp.content_length = 100
        await resp.prepare(request)
        await resp.write(b"short")
        assert request.transport is not None
        request.transport.close()
        return resp

    app = web.Application()
    app.router.add_get("/", handler)
    client = await aiohttp_client(app, trace_configs=[recorder.trace_config])

    resp = await client.get("/")
    # The request itself succeeded: on_request_end already fired.
    assert recorder.terminals == ["request_end"]
    async with resp:
        with pytest.raises(aiohttp.ClientPayloadError):
            await resp.read()
    # A body stream failure after on_request_end emits no further terminal.
    assert recorder.terminals == ["request_end"]
    assert "request_exception" not in recorder.signals


async def test_lifecycle_session_close_with_unread_response(
    aiohttp_client: AiohttpClient,
) -> None:
    recorder = TraceRecorder()

    async def handler(request: web.Request) -> web.Response:
        return web.Response(body=b"unread")

    app = web.Application()
    app.router.add_get("/", handler)
    client = await aiohttp_client(app, trace_configs=[recorder.trace_config])

    resp = await client.get("/")
    assert recorder.terminals == ["request_end"]
    # Closing the session with an unread response emits no further signal.
    await client.session.close()
    assert recorder.terminals == ["request_end"]
    assert "request_exception" not in recorder.signals
    resp.close()


async def test_lifecycle_connection_reuse(aiohttp_client: AiohttpClient) -> None:
    recorder = TraceRecorder()

    async def handler(request: web.Request) -> web.Response:
        return web.Response(body=b"ok")

    app = web.Application()
    app.router.add_get("/", handler)
    client = await aiohttp_client(app, trace_configs=[recorder.trace_config])

    async with client.get("/") as resp:
        await resp.read()
    recorder.events.clear()

    async with client.get("/") as resp:
        await resp.read()

    signals = recorder.signals
    assert "connection_reuseconn" in signals
    assert "connection_create_start" not in signals
    assert recorder.terminals == ["request_end"]
    recorder.assert_single_ctx()


async def test_lifecycle_concurrent_requests_ctx_isolated(
    aiohttp_client: AiohttpClient,
) -> None:
    recorder = TraceRecorder()
    both_inflight = asyncio.Event()
    inflight = 0

    async def handler(request: web.Request) -> web.Response:
        nonlocal inflight
        inflight += 1
        if inflight == 2:
            both_inflight.set()
        await both_inflight.wait()
        return web.Response(body=b"ok")

    app = web.Application()
    app.router.add_get("/", handler)
    client = await aiohttp_client(app, trace_configs=[recorder.trace_config])

    user_ctx1 = object()
    user_ctx2 = object()
    resp1, resp2 = await asyncio.gather(
        client.get("/", trace_request_ctx=user_ctx1),
        client.get("/", trace_request_ctx=user_ctx2),
    )
    async with resp1:
        await resp1.read()
    async with resp2:
        await resp2.read()

    # Each concurrent request got its own trace_config_ctx ...
    ctxs = {ctx for _, _, ctx in recorder.events}
    assert len(ctxs) == 2
    # ... and each of them replays a complete, successful lifecycle.
    for ctx in ctxs:
        group = [name for name, _, event_ctx in recorder.events if event_ctx is ctx]
        assert "request_start" in group
        assert "request_end" in group
        assert "request_exception" not in group
    # User contexts are not shared between concurrent requests.
    seen_user_ctxs = {ctx.trace_request_ctx for ctx in ctxs}
    assert seen_user_ctxs == {user_ctx1, user_ctx2}


async def test_lifecycle_multiple_trace_configs(aiohttp_client: AiohttpClient) -> None:
    recorder1 = TraceRecorder()
    recorder2 = TraceRecorder()

    async def handler(request: web.Request) -> web.Response:
        return web.Response(body=b"ok")

    app = web.Application()
    app.router.add_get("/", handler)
    client = await aiohttp_client(
        app, trace_configs=[recorder1.trace_config, recorder2.trace_config]
    )

    async with client.get("/") as resp:
        await resp.read()

    # Both TraceConfigs observe the same lifecycle ...
    assert recorder1.signals == recorder2.signals
    assert recorder1.terminals == ["request_end"]
    assert recorder2.terminals == ["request_end"]
    # ... but neither overwrites the other's trace_config_ctx.
    assert recorder1.events[0][2] is not recorder2.events[0][2]
    recorder1.assert_single_ctx()
    recorder2.assert_single_ctx()


async def test_lifecycle_callback_failure_does_not_change_result(
    aiohttp_client: AiohttpClient,
    caplog: pytest.LogCaptureFixture,
) -> None:
    recorder = TraceRecorder()

    async def fail(session: ClientSession, ctx: Any, params: Any) -> None:
        raise RuntimeError("callback boom")

    failing = aiohttp.TraceConfig()
    failing.on_request_start.append(fail)
    failing.on_request_end.append(fail)
    failing.on_response_chunk_received.append(fail)

    async def handler(request: web.Request) -> web.Response:
        return web.Response(body=b"ok")

    app = web.Application()
    app.router.add_get("/", handler)
    client = await aiohttp_client(app, trace_configs=[failing, recorder.trace_config])

    with caplog.at_level(logging.ERROR, logger="aiohttp.client"):
        async with client.get("/") as resp:
            assert resp.status == 200
            assert await resp.read() == b"ok"

    # The request succeeded and the other TraceConfig saw a full lifecycle.
    assert recorder.terminals == ["request_end"]
    assert "request_exception" not in recorder.signals
    # Callback failures are reportable through the aiohttp.client logger.
    assert "on_request_start" in caplog.text
    assert "on_request_end" in caplog.text
    failed_signals = {
        record.name for record in caplog.records if record.exc_info is not None
    }
    assert failed_signals == {"aiohttp.client"}


async def test_lifecycle_exception_callback_failure_preserves_error(
    caplog: pytest.LogCaptureFixture,
) -> None:
    recorder = TraceRecorder()

    async def fail(session: ClientSession, ctx: Any, params: Any) -> None:
        raise RuntimeError("callback boom")

    failing = aiohttp.TraceConfig()
    failing.on_request_exception.append(fail)

    connector = TCPConnector(resolver=_FailingResolver())
    async with ClientSession(
        connector=connector, trace_configs=[failing, recorder.trace_config]
    ) as session:
        with caplog.at_level(logging.ERROR, logger="aiohttp.client"):
            with pytest.raises(aiohttp.ClientConnectorError):
                await session.get("http://unresolvable.example/")

    # The original request exception is neither replaced nor duplicated.
    assert recorder.terminals == ["request_exception"]
    assert isinstance(
        recorder.params_of("request_exception").exception,
        aiohttp.ClientConnectorError,
    )
    assert "on_request_exception" in caplog.text


async def test_lifecycle_cancel_during_request_end_callback(
    aiohttp_client: AiohttpClient,
) -> None:
    recorder = TraceRecorder()

    async def cancel_self(session: ClientSession, ctx: Any, params: Any) -> None:
        raise asyncio.CancelledError

    cancelling = aiohttp.TraceConfig()
    cancelling.on_request_end.append(cancel_self)

    async def handler(request: web.Request) -> web.Response:
        return web.Response(body=b"ok")

    app = web.Application()
    app.router.add_get("/", handler)
    client = await aiohttp_client(
        app, trace_configs=[recorder.trace_config, cancelling]
    )

    with pytest.raises(asyncio.CancelledError):
        await client.get("/")

    # on_request_end committed before the cancellation: no second terminal.
    assert recorder.terminals == ["request_end"]
    assert "request_exception" not in recorder.signals
