import asyncio
import logging
import socket
import sys
from types import SimpleNamespace
from typing import Any, cast
from unittest import mock
from unittest.mock import Mock

import pytest
from aiosignal import Signal
from multidict import CIMultiDict
from yarl import URL

from aiohttp import (
    ClientConnectorDNSError,
    ClientPayloadError,
    ClientSession,
    ConnectionTimeoutError,
    TCPConnector,
    hdrs,
)
from aiohttp.abc import AbstractResolver, ResolveResult
from aiohttp.client_reqrep import ClientRequest, ClientResponse
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
        [
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
        ],
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

    @pytest.mark.parametrize(
        "signal,params",
        [
            ("request_start", (Mock(), Mock(), Mock())),
            ("request_chunk_sent", (Mock(), Mock(), Mock())),
            ("request_end", (Mock(), Mock(), Mock(), Mock())),
            ("request_exception", (Mock(), Mock(), Mock(), Mock())),
            ("request_redirect", (Mock(), Mock(), Mock(), Mock())),
            ("connection_queued_start", ()),
            ("connection_queued_end", ()),
            ("connection_create_start", ()),
            ("connection_create_end", ()),
            ("connection_reuseconn", ()),
            ("dns_resolvehost_start", (Mock(),)),
            ("dns_resolvehost_end", (Mock(),)),
            ("dns_cache_hit", (Mock(),)),
            ("dns_cache_miss", (Mock(),)),
        ],
    )
    async def test_callback_error_isolated(
        self, signal: str, params: tuple[Mock, ...], caplog: pytest.LogCaptureFixture
    ) -> None:
        """A raising callback must not escape lifecycle signals."""
        callback = mock.AsyncMock(side_effect=RuntimeError("boom"))

        trace_config = TraceConfig()
        getattr(trace_config, "on_%s" % signal).append(callback)
        trace_config.freeze()
        trace = Trace(Mock(), trace_config, trace_config.trace_config_ctx())

        with caplog.at_level(logging.ERROR, logger="aiohttp.client"):
            await getattr(trace, "send_%s" % signal)(*params)

        callback.assert_called_once()
        assert "Error in on_%s trace callback" % signal in caplog.text

    async def test_response_chunk_received_callback_error_propagates(self) -> None:
        """The streaming signal is the boundary: errors reach the reader."""
        callback = mock.AsyncMock(side_effect=RuntimeError("boom"))

        trace_config = TraceConfig()
        trace_config.on_response_chunk_received.append(callback)
        trace_config.freeze()
        trace = Trace(Mock(), trace_config, trace_config.trace_config_ctx())

        with pytest.raises(RuntimeError, match="boom"):
            await trace.send_response_chunk_received(Mock(), Mock(), Mock())


class TraceRecorder:
    """Collects trace events so a full request lifecycle can be replayed."""

    TERMINAL_SIGNALS = ("request_end", "request_exception")

    def __init__(self) -> None:
        self.events: list[tuple[str, Any, Any]] = []
        self.config = TraceConfig()
        for name in (
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
        ):
            getattr(self.config, "on_%s" % name).append(self._make_callback(name))

    def _make_callback(self, name: str) -> Any:
        async def callback(session: Any, ctx: Any, params: Any) -> None:
            self.events.append((name, params, ctx))

        return callback

    @property
    def names(self) -> list[str]:
        return [name for name, _, _ in self.events]

    @property
    def terminal(self) -> list[str]:
        return [name for name in self.names if name in self.TERMINAL_SIGNALS]

    def params_of(self, name: str) -> list[Any]:
        return [params for event, params, _ in self.events if event == name]

    def assert_single_ctx(self) -> Any:
        ctxs = {id(ctx) for _, _, ctx in self.events}
        assert len(ctxs) == 1
        return self.events[0][2]


def _make_response(status: int = 200, headers: dict[str, str] | None = None) -> Mock:
    resp = Mock()
    resp.status = status
    resp.headers = CIMultiDict(headers or {})
    resp._raw_cookie_headers = None
    resp.connection = None
    return resp


class TestTraceLifecycle:
    """The event log of one request must replay a complete lifecycle."""

    async def test_success(self) -> None:
        recorder = TraceRecorder()
        resp = _make_response()

        async def handler(req: ClientRequest) -> ClientResponse:
            return cast(ClientResponse, resp)

        async with ClientSession(trace_configs=[recorder.config]) as session:
            with mock.patch("aiohttp.client._connect_and_send_request", new=handler):
                assert await session.get("http://example.com/") is resp

        assert recorder.names == ["request_start", "request_end"]
        assert recorder.terminal == ["request_end"]
        assert recorder.params_of("request_end")[0].response is resp
        recorder.assert_single_ctx()
        for _, params, _ in recorder.events:
            assert params.url == URL("http://example.com/")

    async def test_redirect(self) -> None:
        recorder = TraceRecorder()
        redirect_resp = _make_response(301, {hdrs.LOCATION: "/other"})
        final_resp = _make_response()
        responses = iter([redirect_resp, final_resp])

        async def handler(req: ClientRequest) -> ClientResponse:
            return cast(ClientResponse, next(responses))

        async with ClientSession(trace_configs=[recorder.config]) as session:
            with mock.patch("aiohttp.client._connect_and_send_request", new=handler):
                assert await session.get("http://example.com/redirect") is final_resp

        assert recorder.names == ["request_start", "request_redirect", "request_end"]
        assert recorder.terminal == ["request_end"]
        # The ctx is stable across redirect hops of the same request.
        recorder.assert_single_ctx()
        assert recorder.params_of("request_start")[0].url == URL(
            "http://example.com/redirect"
        )
        assert recorder.params_of("request_redirect")[0].url == URL(
            "http://example.com/redirect"
        )
        assert recorder.params_of("request_end")[0].url == URL(
            "http://example.com/other"
        )

    async def test_dns_failure(self) -> None:
        class FailingResolver(AbstractResolver):
            async def resolve(
                self, host: str, port: int = 0, family: int = socket.AF_INET
            ) -> list[ResolveResult]:
                raise OSError("DNS lookup failed")

            def close(self) -> None:
                pass

        recorder = TraceRecorder()
        connector = TCPConnector(resolver=FailingResolver())
        async with ClientSession(
            connector=connector, trace_configs=[recorder.config]
        ) as session:
            with pytest.raises(ClientConnectorDNSError):
                await session.get("http://fake.example.com/")

        assert recorder.names == [
            "request_start",
            "connection_create_start",
            "dns_cache_miss",
            "dns_resolvehost_start",
            "request_exception",
        ]
        assert recorder.terminal == ["request_exception"]
        recorder.assert_single_ctx()
        exc = recorder.params_of("request_exception")[0].exception
        assert isinstance(exc, ClientConnectorDNSError)
        assert exc.host == "fake.example.com"
        assert exc.port == 80
        assert exc.ssl is True

    async def test_cancellation(self) -> None:
        recorder = TraceRecorder()
        started = asyncio.Event()
        hang = asyncio.Event()

        async def handler(req: ClientRequest) -> ClientResponse:
            started.set()
            await hang.wait()
            raise AssertionError("unreachable")

        async with ClientSession(trace_configs=[recorder.config]) as session:
            with mock.patch("aiohttp.client._connect_and_send_request", new=handler):
                task = asyncio.ensure_future(session.get("http://example.com/"))
                await started.wait()
                task.cancel()
                with pytest.raises(asyncio.CancelledError):
                    await task

        assert recorder.names == ["request_start", "request_exception"]
        assert recorder.terminal == ["request_exception"]
        recorder.assert_single_ctx()
        exc = recorder.params_of("request_exception")[0].exception
        assert isinstance(exc, asyncio.CancelledError)

    async def test_connector_timeout(self) -> None:
        recorder = TraceRecorder()
        async with ClientSession(trace_configs=[recorder.config]) as session:
            with mock.patch("aiohttp.client.TCPConnector.connect") as connect:
                connect.side_effect = asyncio.TimeoutError()
                with pytest.raises(ConnectionTimeoutError):
                    await session.get("http://example.com/")

        assert recorder.names == ["request_start", "request_exception"]
        assert recorder.terminal == ["request_exception"]
        exc = recorder.params_of("request_exception")[0].exception
        assert isinstance(exc, ConnectionTimeoutError)

    async def test_stream_error_after_request_end(self) -> None:
        """A body read failure is not a second terminal signal."""
        recorder = TraceRecorder()
        resp = _make_response()
        resp.read = mock.AsyncMock(side_effect=ClientPayloadError("broken"))

        async def handler(req: ClientRequest) -> ClientResponse:
            return cast(ClientResponse, resp)

        async with ClientSession(trace_configs=[recorder.config]) as session:
            with mock.patch("aiohttp.client._connect_and_send_request", new=handler):
                resp = await session.get("http://example.com/")
                with pytest.raises(ClientPayloadError, match="broken"):
                    await resp.read()

        assert recorder.names == ["request_start", "request_end"]
        assert recorder.terminal == ["request_end"]

    async def test_unread_response_session_close(self) -> None:
        """Closing the session with an unread response adds no signal."""
        recorder = TraceRecorder()

        async def handler(req: ClientRequest) -> ClientResponse:
            return cast(ClientResponse, _make_response())

        async with ClientSession(trace_configs=[recorder.config]) as session:
            with mock.patch("aiohttp.client._connect_and_send_request", new=handler):
                await session.get("http://example.com/")
                # The response body is never read before the session closes.

        assert recorder.names == ["request_start", "request_end"]
        assert recorder.terminal == ["request_end"]

    async def test_callback_error_does_not_change_result(
        self, caplog: pytest.LogCaptureFixture
    ) -> None:
        recorder = TraceRecorder()
        resp = _make_response()

        async def bad_end(session: object, ctx: object, params: object) -> None:
            raise RuntimeError("boom")

        recorder.config.on_request_end.append(bad_end)

        async def handler(req: ClientRequest) -> ClientResponse:
            return cast(ClientResponse, resp)

        async with ClientSession(trace_configs=[recorder.config]) as session:
            with mock.patch("aiohttp.client._connect_and_send_request", new=handler):
                with caplog.at_level(logging.ERROR, logger="aiohttp.client"):
                    assert await session.get("http://example.com/") is resp

        assert recorder.terminal == ["request_end"]
        assert "request_exception" not in recorder.names
        assert "Error in on_request_end trace callback" in caplog.text

    async def test_callback_error_does_not_mask_request_exception(
        self, caplog: pytest.LogCaptureFixture
    ) -> None:
        recorder = TraceRecorder()
        original = ValueError("original")

        async def bad_exception(session: object, ctx: object, params: object) -> None:
            raise RuntimeError("callback boom")

        recorder.config.on_request_exception.append(bad_exception)

        async with ClientSession(trace_configs=[recorder.config]) as session:
            with mock.patch("aiohttp.client.TCPConnector.connect") as connect:
                connect.side_effect = original
                with caplog.at_level(logging.ERROR, logger="aiohttp.client"):
                    with pytest.raises(ValueError, match="original"):
                        await session.get("http://example.com/")

        assert recorder.terminal == ["request_exception"]
        assert recorder.params_of("request_exception")[0].exception is original
        assert "Error in on_request_exception trace callback" in caplog.text

    async def test_multiple_trace_configs_isolated(self) -> None:
        recorder_a = TraceRecorder()
        recorder_b = TraceRecorder()

        async def bad_start(session: object, ctx: object, params: object) -> None:
            raise RuntimeError("boom")

        recorder_a.config.on_request_start.append(bad_start)

        async def handler(req: ClientRequest) -> ClientResponse:
            return cast(ClientResponse, _make_response())

        async with ClientSession(
            trace_configs=[recorder_a.config, recorder_b.config]
        ) as session:
            with mock.patch("aiohttp.client._connect_and_send_request", new=handler):
                await session.get("http://example.com/")

        # A failing callback in one config does not affect the other config.
        for recorder in (recorder_a, recorder_b):
            assert recorder.names == ["request_start", "request_end"]
            assert recorder.terminal == ["request_end"]
        # Each config gets its own ctx, never shared or overwritten.
        assert recorder_a.assert_single_ctx() is not recorder_b.assert_single_ctx()

    async def test_concurrent_requests_do_not_share_ctx(self) -> None:
        recorder = TraceRecorder()
        hold = asyncio.Event()

        async def handler(req: ClientRequest) -> ClientResponse:
            await hold.wait()
            return cast(ClientResponse, _make_response())

        async with ClientSession(trace_configs=[recorder.config]) as session:
            with mock.patch("aiohttp.client._connect_and_send_request", new=handler):
                task1 = asyncio.ensure_future(
                    session.get("http://example.com/1", trace_request_ctx={"req_id": 1})
                )
                task2 = asyncio.ensure_future(
                    session.get("http://example.com/2", trace_request_ctx={"req_id": 2})
                )
                await asyncio.sleep(0)
                hold.set()
                await asyncio.gather(task1, task2)

        by_ctx: dict[int, list[tuple[str, Any, Any]]] = {}
        for event in recorder.events:
            by_ctx.setdefault(id(event[2]), []).append(event)
        # The user context of one request is never visible to the other.
        assert len(by_ctx) == 2
        for events in by_ctx.values():
            ctx = events[0][2]
            req_id = ctx.trace_request_ctx["req_id"]
            for name, params, _ in events:
                assert str(params.url) == "http://example.com/%s" % req_id
            terminals = [
                name for name, _, _ in events if name in TraceRecorder.TERMINAL_SIGNALS
            ]
            assert terminals == ["request_end"]

    async def test_cancel_during_request_start_completes_lifecycle(self) -> None:
        recorder_a = TraceRecorder()
        recorder_b = TraceRecorder()

        async def cancel_self(session: object, ctx: object, params: object) -> None:
            task = asyncio.current_task()
            assert task is not None
            task.cancel()
            await asyncio.sleep(0)

        recorder_a.config.on_request_start.append(cancel_self)

        async with ClientSession(
            trace_configs=[recorder_a.config, recorder_b.config]
        ) as session:
            with pytest.raises(asyncio.CancelledError):
                await session.get("http://example.com/")

        # The start signal still reaches every config before the terminal one.
        for recorder in (recorder_a, recorder_b):
            assert recorder.names == ["request_start", "request_exception"]
            exc = recorder.params_of("request_exception")[0].exception
            assert isinstance(exc, asyncio.CancelledError)

    async def test_cancel_during_request_end_is_single_terminal(self) -> None:
        recorder_a = TraceRecorder()
        recorder_b = TraceRecorder()

        async def cancel_self(session: object, ctx: object, params: object) -> None:
            task = asyncio.current_task()
            assert task is not None
            task.cancel()
            await asyncio.sleep(0)

        recorder_a.config.on_request_end.append(cancel_self)

        async def handler(req: ClientRequest) -> ClientResponse:
            return cast(ClientResponse, _make_response())

        async with ClientSession(
            trace_configs=[recorder_a.config, recorder_b.config]
        ) as session:
            with mock.patch("aiohttp.client._connect_and_send_request", new=handler):
                with pytest.raises(asyncio.CancelledError):
                    await session.get("http://example.com/")

        # Cancellation while delivering on_request_end must not produce a
        # duplicate on_request_exception: the terminal signal stays single.
        for recorder in (recorder_a, recorder_b):
            assert recorder.names == ["request_start", "request_end"]
            assert recorder.terminal == ["request_end"]
