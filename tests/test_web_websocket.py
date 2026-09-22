import asyncio
import json
import time
from typing import Protocol
from unittest import mock

import aiosignal
import pytest
from multidict import CIMultiDict
from pytest_mock import MockerFixture

from aiohttp import WSCloseCode, WSMessageTypeError, WSMsgType, web
from aiohttp.http import WS_CLOSED_MESSAGE, WS_CLOSING_MESSAGE
from aiohttp.http_websocket import (
    WebSocketError,
    WSMessageClose,
    WSMessageContinuation,
    WSMessageText,
)
from aiohttp.streams import EofStream
from aiohttp.test_utils import make_mocked_request
from aiohttp.web_ws import WebSocketReady


class _RequestMaker(Protocol):
    def __call__(
        self,
        method: str,
        path: str,
        headers: CIMultiDict[str] | None = None,
        protocols: bool = False,
    ) -> web.Request: ...


@pytest.fixture
def app() -> web.Application:
    ret: web.Application = mock.create_autospec(web.Application, spec_set=True)
    ret.on_response_prepare = aiosignal.Signal(ret)  # type: ignore[misc]
    ret.on_response_prepare.freeze()
    return ret


@pytest.fixture
def protocol() -> web.RequestHandler[web.Request]:
    ret = mock.Mock()
    ret.set_parser.return_value = ret
    ret._timeout_ceil_threshold = 5
    return ret


@pytest.fixture
def make_request(
    app: web.Application, protocol: web.RequestHandler[web.Request]
) -> _RequestMaker:
    def maker(
        method: str,
        path: str,
        headers: CIMultiDict[str] | None = None,
        protocols: bool = False,
    ) -> web.Request:
        if headers is None:
            headers = CIMultiDict(
                {
                    "HOST": "server.example.com",
                    "UPGRADE": "websocket",
                    "CONNECTION": "Upgrade",
                    "SEC-WEBSOCKET-KEY": "dGhlIHNhbXBsZSBub25jZQ==",
                    "ORIGIN": "http://example.com",
                    "SEC-WEBSOCKET-VERSION": "13",
                }
            )
        if protocols:
            headers["SEC-WEBSOCKET-PROTOCOL"] = "chat, superchat"

        return make_mocked_request(method, path, headers, app=app, protocol=protocol)

    return maker


async def test_nonstarted_ping() -> None:
    ws = web.WebSocketResponse()
    with pytest.raises(RuntimeError):
        await ws.ping()


async def test_nonstarted_pong() -> None:
    ws = web.WebSocketResponse()
    with pytest.raises(RuntimeError):
        await ws.pong()


async def test_nonstarted_send_frame() -> None:
    ws = web.WebSocketResponse()
    with pytest.raises(RuntimeError):
        await ws.send_frame(b"string", WSMsgType.TEXT)


async def test_nonstarted_send_str() -> None:
    ws = web.WebSocketResponse()
    with pytest.raises(RuntimeError):
        await ws.send_str("string")


async def test_nonstarted_send_bytes() -> None:
    ws = web.WebSocketResponse()
    with pytest.raises(RuntimeError):
        await ws.send_bytes(b"bytes")


async def test_nonstarted_send_json() -> None:
    ws = web.WebSocketResponse()
    with pytest.raises(RuntimeError):
        await ws.send_json({"type": "json"})


async def test_nonstarted_close() -> None:
    ws = web.WebSocketResponse()
    with pytest.raises(RuntimeError):
        await ws.close()


async def test_nonstarted_receive_str() -> None:
    ws = web.WebSocketResponse()
    with pytest.raises(RuntimeError):
        await ws.receive_str()


async def test_cancel_heartbeat_cancels_pending_heartbeat_reset_handle() -> None:
    ws = web.WebSocketResponse(heartbeat=0.05)
    ws._loop = asyncio.get_running_loop()
    ws._on_data_received()
    handle = ws._heartbeat_reset_handle
    assert handle is not None

    ws._cancel_heartbeat()

    assert ws._heartbeat_reset_handle is None
    assert ws._need_heartbeat_reset is False
    assert handle.cancelled()


async def test_flush_heartbeat_reset_returns_early_when_not_needed() -> None:
    ws = web.WebSocketResponse(heartbeat=0.05)
    ws._need_heartbeat_reset = False

    with mock.patch.object(ws, "_reset_heartbeat") as reset:
        ws._flush_heartbeat_reset()
        reset.assert_not_called()


async def test_send_heartbeat_returns_early_when_reset_is_pending() -> None:
    ws = web.WebSocketResponse(heartbeat=0.05)
    ws._need_heartbeat_reset = True

    ws._send_heartbeat()

    assert ws._pong_response_cb is None
    assert ws._ping_task is None


async def test_nonstarted_receive_bytes() -> None:
    ws = web.WebSocketResponse()
    with pytest.raises(RuntimeError):
        await ws.receive_bytes()


async def test_nonstarted_receive_json() -> None:
    ws = web.WebSocketResponse()
    with pytest.raises(RuntimeError):
        await ws.receive_json()


async def test_send_str_nonstring(make_request: _RequestMaker) -> None:
    req = make_request("GET", "/")
    ws = web.WebSocketResponse()
    await ws.prepare(req)
    with pytest.raises(TypeError):
        await ws.send_str(b"bytes")  # type: ignore[arg-type]


async def test_send_bytes_nonbytes(make_request: _RequestMaker) -> None:
    req = make_request("GET", "/")
    ws = web.WebSocketResponse()
    await ws.prepare(req)
    with pytest.raises(TypeError):
        await ws.send_bytes("string")  # type: ignore[arg-type]


async def test_send_json_nonjson(make_request: _RequestMaker) -> None:
    req = make_request("GET", "/")
    ws = web.WebSocketResponse()
    await ws.prepare(req)
    with pytest.raises(TypeError):
        await ws.send_json(set())


async def test_nonstarted_send_json_bytes() -> None:
    ws = web.WebSocketResponse()
    with pytest.raises(RuntimeError):
        await ws.send_json_bytes(
            {"type": "json"}, dumps=lambda x: json.dumps(x).encode("utf-8")
        )


async def test_send_json_bytes_nonjson(make_request: _RequestMaker) -> None:
    req = make_request("GET", "/")
    ws = web.WebSocketResponse()
    await ws.prepare(req)
    with pytest.raises(TypeError):
        await ws.send_json_bytes(set(), dumps=lambda x: json.dumps(x).encode("utf-8"))

    assert ws._reader is not None
    ws._reader.feed_data(WS_CLOSED_MESSAGE)
    await ws.close()


async def test_write_non_prepared() -> None:
    ws = web.WebSocketResponse()
    with pytest.raises(RuntimeError):
        await ws.write(b"data")


async def test_heartbeat_timeout(make_request: _RequestMaker) -> None:
    """Verify the transport is closed when the heartbeat timeout is reached."""
    future = asyncio.get_running_loop().create_future()
    req = make_request("GET", "/")
    assert req.transport is not None
    req.transport.close.side_effect = lambda: future.set_result(None)  # type: ignore[attr-defined]
    lowest_time = time.get_clock_info("monotonic").resolution
    req._protocol._timeout_ceil_threshold = lowest_time
    ws = web.WebSocketResponse(heartbeat=lowest_time, timeout=lowest_time)
    await ws.prepare(req)
    await future
    assert ws.closed


async def test_heartbeat_reset_coalesces_on_data(
    make_request: _RequestMaker,
) -> None:
    req = make_request("GET", "/")
    ws = web.WebSocketResponse(heartbeat=0.05)
    await ws.prepare(req)

    with mock.patch.object(ws, "_reset_heartbeat") as reset:
        ws._on_data_received()
        ws._on_data_received()

        await asyncio.sleep(0)

        assert reset.call_count == 1


async def test_receive_does_not_reset_heartbeat() -> None:
    ws = web.WebSocketResponse(heartbeat=0.05)
    msg = mock.Mock(type=WSMsgType.TEXT)
    reader = mock.Mock()
    reader.read = mock.AsyncMock(return_value=msg)
    ws._reader = reader

    with mock.patch.object(ws, "_reset_heartbeat") as reset:
        received = await ws.receive()

    assert received is msg
    reset.assert_not_called()


def test_websocket_ready() -> None:
    websocket_ready = WebSocketReady(True, "chat")
    assert websocket_ready.ok is True
    assert websocket_ready.protocol == "chat"


def test_websocket_not_ready() -> None:
    websocket_ready = WebSocketReady(False, None)
    assert websocket_ready.ok is False
    assert websocket_ready.protocol is None


def test_websocket_ready_unknown_protocol() -> None:
    websocket_ready = WebSocketReady(True, None)
    assert websocket_ready.ok is True
    assert websocket_ready.protocol is None


def test_bool_websocket_ready() -> None:
    websocket_ready = WebSocketReady(True, None)
    assert bool(websocket_ready) is True


def test_bool_websocket_not_ready() -> None:
    websocket_ready = WebSocketReady(False, None)
    assert bool(websocket_ready) is False


def test_can_prepare_ok(make_request: _RequestMaker) -> None:
    req = make_request("GET", "/", protocols=True)
    ws = web.WebSocketResponse(protocols=("chat",))
    assert WebSocketReady(True, "chat") == ws.can_prepare(req)


def test_can_prepare_unknown_protocol(make_request: _RequestMaker) -> None:
    req = make_request("GET", "/")
    ws = web.WebSocketResponse()
    assert WebSocketReady(True, None) == ws.can_prepare(req)


def test_can_prepare_without_upgrade(make_request: _RequestMaker) -> None:
    req = make_request("GET", "/", headers=CIMultiDict({}))
    ws = web.WebSocketResponse()
    assert WebSocketReady(False, None) == ws.can_prepare(req)


async def test_can_prepare_started(make_request: _RequestMaker) -> None:
    req = make_request("GET", "/")
    ws = web.WebSocketResponse()
    await ws.prepare(req)
    with pytest.raises(RuntimeError) as ctx:
        ws.can_prepare(req)

    assert "Already started" in str(ctx.value)


def test_closed_after_ctor() -> None:
    ws = web.WebSocketResponse()
    assert not ws.closed
    assert ws.close_code is None


async def test_raise_writer_limit(make_request: _RequestMaker) -> None:
    """Test the writer limit can be adjusted."""
    req = make_request("GET", "/")
    ws = web.WebSocketResponse(writer_limit=1234567)
    await ws.prepare(req)
    assert ws._reader is not None
    assert ws._writer is not None
    assert ws._writer._limit == 1234567
    ws._reader.feed_data(WS_CLOSED_MESSAGE)
    await ws.close()


async def test_send_str_closed(make_request: _RequestMaker) -> None:
    req = make_request("GET", "/")
    ws = web.WebSocketResponse()
    await ws.prepare(req)
    assert ws._reader is not None
    ws._reader.feed_data(WS_CLOSED_MESSAGE)
    await ws.close()
    assert req.transport is not None
    assert len(req.transport.close.mock_calls) == 1  # type: ignore[attr-defined]

    with pytest.raises(ConnectionError):
        await ws.send_str("string")


async def test_recv_str_closed(make_request: _RequestMaker) -> None:
    req = make_request("GET", "/")
    ws = web.WebSocketResponse()
    await ws.prepare(req)
    assert ws._reader is not None
    ws._reader.feed_data(WS_CLOSED_MESSAGE)
    await ws.close()

    with pytest.raises(
        WSMessageTypeError,
        match=f"Received message {WSMsgType.CLOSED}:.+ is not WSMsgType.TEXT",
    ):
        await ws.receive_str()


async def test_send_bytes_closed(make_request: _RequestMaker) -> None:
    req = make_request("GET", "/")
    ws = web.WebSocketResponse()
    await ws.prepare(req)
    assert ws._reader is not None
    ws._reader.feed_data(WS_CLOSED_MESSAGE)
    await ws.close()

    with pytest.raises(ConnectionError):
        await ws.send_bytes(b"bytes")


async def test_recv_bytes_closed(make_request: _RequestMaker) -> None:
    req = make_request("GET", "/")
    ws = web.WebSocketResponse()
    await ws.prepare(req)
    assert ws._reader is not None
    ws._reader.feed_data(WS_CLOSED_MESSAGE)
    await ws.close()

    with pytest.raises(
        WSMessageTypeError,
        match=f"Received message {WSMsgType.CLOSED}:.+ is not WSMsgType.BINARY",
    ):
        await ws.receive_bytes()


async def test_send_json_closed(make_request: _RequestMaker) -> None:
    req = make_request("GET", "/")
    ws = web.WebSocketResponse()
    await ws.prepare(req)
    assert ws._reader is not None
    ws._reader.feed_data(WS_CLOSED_MESSAGE)
    await ws.close()

    with pytest.raises(ConnectionError):
        await ws.send_json({"type": "json"})


async def test_send_json_bytes_closed(make_request: _RequestMaker) -> None:
    req = make_request("GET", "/")
    ws = web.WebSocketResponse()
    await ws.prepare(req)
    assert ws._reader is not None
    ws._reader.feed_data(WS_CLOSED_MESSAGE)
    await ws.close()

    with pytest.raises(ConnectionError):
        await ws.send_json_bytes(
            {"type": "json"}, dumps=lambda x: json.dumps(x).encode("utf-8")
        )


async def test_send_frame_closed(make_request: _RequestMaker) -> None:
    req = make_request("GET", "/")
    ws = web.WebSocketResponse()
    await ws.prepare(req)
    assert ws._reader is not None
    ws._reader.feed_data(WS_CLOSED_MESSAGE)
    await ws.close()

    with pytest.raises(ConnectionError):
        await ws.send_frame(b'{"type": "json"}', WSMsgType.TEXT)


async def test_ping_closed(make_request: _RequestMaker) -> None:
    req = make_request("GET", "/")
    ws = web.WebSocketResponse()
    await ws.prepare(req)
    assert ws._reader is not None
    ws._reader.feed_data(WS_CLOSED_MESSAGE)
    await ws.close()

    with pytest.raises(ConnectionError):
        await ws.ping()


async def test_pong_closed(make_request: _RequestMaker, mocker: MockerFixture) -> None:
    req = make_request("GET", "/")
    ws = web.WebSocketResponse()
    await ws.prepare(req)
    assert ws._reader is not None
    ws._reader.feed_data(WS_CLOSED_MESSAGE)
    await ws.close()

    with pytest.raises(ConnectionError):
        await ws.pong()


async def test_close_idempotent(make_request: _RequestMaker) -> None:
    req = make_request("GET", "/")
    ws = web.WebSocketResponse()
    await ws.prepare(req)
    assert ws._reader is not None
    ws._reader.feed_data(WS_CLOSED_MESSAGE)
    close_code = await ws.close(code=1, message=b"message1")
    assert close_code == 1
    assert ws.closed
    assert req.transport is not None
    assert len(req.transport.close.mock_calls) == 1  # type: ignore[attr-defined]

    close_code = await ws.close(code=2, message=b"message2")
    assert close_code == 0


async def test_prepare_post_method_ok(make_request: _RequestMaker) -> None:
    req = make_request("POST", "/")
    ws = web.WebSocketResponse()
    await ws.prepare(req)
    assert ws.prepared


async def test_prepare_without_upgrade(make_request: _RequestMaker) -> None:
    req = make_request("GET", "/", headers=CIMultiDict({}))
    ws = web.WebSocketResponse()
    with pytest.raises(web.HTTPBadRequest):
        await ws.prepare(req)


async def test_wait_closed_before_start() -> None:
    ws = web.WebSocketResponse()
    with pytest.raises(RuntimeError):
        await ws.close()


async def test_write_eof_not_started() -> None:
    ws = web.WebSocketResponse()
    with pytest.raises(RuntimeError):
        await ws.write_eof()


async def test_write_eof_idempotent(make_request: _RequestMaker) -> None:
    req = make_request("GET", "/")
    ws = web.WebSocketResponse()
    await ws.prepare(req)
    assert req.transport is not None
    assert len(req.transport.close.mock_calls) == 0  # type: ignore[attr-defined]

    assert ws._reader is not None
    ws._reader.feed_data(WS_CLOSED_MESSAGE)
    await ws.close()

    await ws.write_eof()
    await ws.write_eof()
    await ws.write_eof()
    assert len(req.transport.close.mock_calls) == 1  # type: ignore[attr-defined]


async def test_receive_eofstream_in_reader(make_request: _RequestMaker) -> None:
    req = make_request("GET", "/")
    ws = web.WebSocketResponse()
    await ws.prepare(req)

    ws._reader = mock.Mock()
    exc = EofStream()
    ws._reader.read = mock.AsyncMock(side_effect=exc)
    assert ws._payload_writer is not None
    f = asyncio.get_running_loop().create_future()
    f.set_result(True)
    ws._payload_writer.drain.return_value = f  # type: ignore[attr-defined]
    msg = await ws.receive()
    assert msg.type == WSMsgType.CLOSED
    assert ws.closed


async def test_receive_exception_in_reader(make_request: _RequestMaker) -> None:
    req = make_request("GET", "/")
    ws = web.WebSocketResponse()
    await ws.prepare(req)

    ws._reader = mock.Mock()
    exc = Exception()
    ws._reader.read = mock.AsyncMock(side_effect=exc)

    f = asyncio.get_running_loop().create_future()
    assert ws._payload_writer is not None
    ws._payload_writer.drain.return_value = f  # type: ignore[attr-defined]
    f.set_result(True)
    msg = await ws.receive()
    assert msg.type == WSMsgType.ERROR
    assert ws.closed
    assert req.transport is not None
    assert len(req.transport.close.mock_calls) == 1  # type: ignore[attr-defined]


async def test_receive_close_but_left_open(make_request: _RequestMaker) -> None:
    req = make_request("GET", "/")
    ws = web.WebSocketResponse()
    await ws.prepare(req)
    close_message = WSMessageClose(data=1000, size=0, extra="close")

    ws._reader = mock.Mock()
    ws._reader.read = mock.AsyncMock(return_value=close_message)

    f = asyncio.get_running_loop().create_future()
    assert ws._payload_writer is not None
    ws._payload_writer.drain.return_value = f  # type: ignore[attr-defined]
    f.set_result(True)
    msg = await ws.receive()
    assert msg.type == WSMsgType.CLOSE
    assert ws.closed
    assert req.transport is not None
    assert len(req.transport.close.mock_calls) == 1  # type: ignore[attr-defined]


async def test_receive_closing(make_request: _RequestMaker) -> None:
    req = make_request("GET", "/")
    ws = web.WebSocketResponse()
    await ws.prepare(req)
    closing_message = WS_CLOSING_MESSAGE

    ws._reader = mock.Mock()
    read_mock = mock.AsyncMock(return_value=closing_message)
    ws._reader.read = read_mock

    f = asyncio.get_running_loop().create_future()
    assert ws._payload_writer is not None
    ws._payload_writer.drain.return_value = f  # type: ignore[attr-defined]
    f.set_result(True)
    msg = await ws.receive()
    assert msg.type == WSMsgType.CLOSING
    assert not ws.closed

    msg = await ws.receive()
    assert msg.type == WSMsgType.CLOSING
    assert not ws.closed

    ws._cancel(ConnectionResetError("Connection lost"))

    msg = await ws.receive()
    assert msg.type == WSMsgType.CLOSING


async def test_close_after_closing(make_request: _RequestMaker) -> None:
    req = make_request("GET", "/")
    ws = web.WebSocketResponse()
    await ws.prepare(req)
    closing_message = WS_CLOSING_MESSAGE

    ws._reader = mock.Mock()
    ws._reader.read = mock.AsyncMock(return_value=closing_message)

    f = asyncio.get_running_loop().create_future()
    assert ws._payload_writer is not None
    ws._payload_writer.drain.return_value = f  # type: ignore[attr-defined]
    f.set_result(True)
    msg = await ws.receive()
    assert msg.type == WSMsgType.CLOSING
    assert not ws.closed
    assert req.transport is not None
    assert len(req.transport.close.mock_calls) == 0  # type: ignore[attr-defined]

    await ws.close()
    assert ws.closed
    assert len(req.transport.close.mock_calls) == 1  # type: ignore[unreachable]


async def test_receive_timeouterror(make_request: _RequestMaker) -> None:
    req = make_request("GET", "/")
    ws = web.WebSocketResponse()
    await ws.prepare(req)
    assert req.transport is not None
    assert len(req.transport.close.mock_calls) == 0  # type: ignore[attr-defined]

    ws._reader = mock.Mock()
    ws._reader.read = mock.AsyncMock(side_effect=asyncio.TimeoutError())

    with pytest.raises(asyncio.TimeoutError):
        await ws.receive()

    # Should not close the connection on timeout
    assert len(req.transport.close.mock_calls) == 0  # type: ignore[attr-defined]


async def test_multiple_receive_on_close_connection(
    make_request: _RequestMaker,
) -> None:
    req = make_request("GET", "/")
    ws = web.WebSocketResponse()
    await ws.prepare(req)
    assert ws._reader is not None
    ws._reader.feed_data(WS_CLOSED_MESSAGE)
    await ws.close()

    await ws.receive()
    await ws.receive()
    await ws.receive()
    await ws.receive()

    with pytest.raises(RuntimeError):
        await ws.receive()


async def test_concurrent_receive(make_request: _RequestMaker) -> None:
    req = make_request("GET", "/")
    ws = web.WebSocketResponse()
    await ws.prepare(req)
    ws._waiting = True

    with pytest.raises(RuntimeError):
        await ws.receive()


async def test_close_exc(make_request: _RequestMaker) -> None:
    req = make_request("GET", "/")
    ws = web.WebSocketResponse()
    await ws.prepare(req)
    assert req.transport is not None
    assert len(req.transport.close.mock_calls) == 0  # type: ignore[attr-defined]

    exc = ValueError()
    ws._writer = mock.Mock()
    ws._writer.close.side_effect = exc
    await ws.close()
    assert ws.closed
    assert ws.exception() is exc
    assert len(req.transport.close.mock_calls) == 1  # type: ignore[attr-defined]

    ws._closed = False
    ws._writer.close.side_effect = asyncio.CancelledError()
    with pytest.raises(asyncio.CancelledError):
        await ws.close()


async def test_prepare_twice_idempotent(make_request: _RequestMaker) -> None:
    req = make_request("GET", "/")
    ws = web.WebSocketResponse()

    impl1 = await ws.prepare(req)
    impl2 = await ws.prepare(req)
    assert impl1 is impl2


async def test_send_with_per_message_deflate(
    make_request: _RequestMaker, mocker: MockerFixture
) -> None:
    req = make_request("GET", "/")
    ws = web.WebSocketResponse()
    await ws.prepare(req)
    with mock.patch.object(ws._writer, "send_frame", autospec=True, spec_set=True) as m:
        await ws.send_str("string", compress=15)
        m.assert_called_with(b"string", WSMsgType.TEXT, compress=15)

        await ws.send_bytes(b"bytes", compress=0)
        m.assert_called_with(b"bytes", WSMsgType.BINARY, compress=0)

        await ws.send_json("[{}]", compress=9)
        m.assert_called_with(b'"[{}]"', WSMsgType.TEXT, compress=9)

        await ws.send_frame(b"[{}]", WSMsgType.TEXT, compress=9)
        m.assert_called_with(b"[{}]", WSMsgType.TEXT, compress=9)


async def test_no_transfer_encoding_header(
    make_request: _RequestMaker, mocker: MockerFixture
) -> None:
    req = make_request("GET", "/")
    ws = web.WebSocketResponse()
    await ws._start(req)

    assert "Transfer-Encoding" not in ws.headers


@pytest.mark.parametrize(
    "ws_transport, expected_result",
    [
        (
            mock.MagicMock(
                transport=mock.MagicMock(
                    get_extra_info=lambda name, default=None: {"test": "existent"}.get(
                        name, default
                    )
                )
            ),
            "existent",
        ),
        (None, "default"),
    ],
)
async def test_get_extra_info(
    make_request: _RequestMaker,
    mocker: MockerFixture,
    ws_transport: mock.MagicMock | None,
    expected_result: str,
) -> None:
    valid_key = "test"
    default_value = "default"

    req = make_request("GET", "/")
    ws = web.WebSocketResponse()

    await ws.prepare(req)
    ws._writer = ws_transport

    assert expected_result == ws.get_extra_info(valid_key, default_value)


def _close_frames_written(transport: mock.Mock) -> list[bytes]:
    """Return the close frames written to the transport so far."""
    return [
        call[0][0]
        for call in transport.write.call_args_list
        if call[0][0][:1] == b"\x88"
    ]


def _assert_heartbeat_reclaimed(ws: web.WebSocketResponse) -> None:
    """Assert no heartbeat timer or task is left behind."""
    assert ws._heartbeat_cb is None
    assert ws._pong_response_cb is None
    assert ws._ping_task is None
    assert ws._heartbeat_reset_handle is None
    assert ws._need_heartbeat_reset is False


async def test_pong_timeout_after_transport_gone(
    make_request: _RequestMaker, protocol: web.RequestHandler[web.Request]
) -> None:
    """A pong timeout firing after EOF still reaches the closed state.

    The connection must never be stuck waiting for a pong that can
    no longer arrive.
    """
    req = make_request("GET", "/")
    ws = web.WebSocketResponse(heartbeat=0.01)
    await ws.prepare(req)
    # Transport is already gone (EOF) when the pong timeout fires.
    protocol.transport = None

    ws._pong_not_received()

    assert ws.closed
    assert ws.close_code == WSCloseCode.ABNORMAL_CLOSURE
    assert isinstance(ws.exception(), asyncio.TimeoutError)
    _assert_heartbeat_reclaimed(ws)


async def test_heartbeat_ping_close_and_eof_single_close_frame(
    make_request: _RequestMaker,
) -> None:
    """Heartbeat ping, client close and EOF racing produce one close frame."""
    req = make_request("GET", "/")
    ws = web.WebSocketResponse(heartbeat=0.01)
    await ws.prepare(req)
    transport = req.transport
    assert transport is not None
    transport.is_closing.return_value = False  # type: ignore[attr-defined]

    # A heartbeat ping is in flight and the pong timeout is armed.
    ws._heartbeat_when = 0.0
    ws._send_heartbeat()
    assert ws._pong_response_cb is not None
    # Let the ping task run so the ping frame goes on the wire.
    await asyncio.sleep(0)
    assert transport.write.call_args_list[0][0][0] == b"\x89\x00"  # type: ignore[attr-defined]

    # The client close and the transport EOF arrive while waiting for pong.
    assert ws._reader is not None
    ws._reader.feed_data(WSMessageClose(data=1000, size=0, extra="close"))
    ws._reader.feed_eof()
    msg = await ws.receive()
    assert msg.type == WSMsgType.CLOSE

    # The pong timeout fires after the close handshake already completed.
    ws._pong_not_received()

    # Exactly one close frame with the echoed code went on the wire.
    close_frames = _close_frames_written(transport)
    assert close_frames == [b"\x88\x02\x03\xe8"]
    assert len(transport.close.mock_calls) == 1  # type: ignore[attr-defined]
    assert ws.closed
    assert ws.close_code == 1000
    _assert_heartbeat_reclaimed(ws)


async def test_eof_then_pong_timeout_then_close_single_terminal(
    make_request: _RequestMaker,
) -> None:
    """EOF first, then pong timeout, then close(): a single terminal state."""
    req = make_request("GET", "/")
    ws = web.WebSocketResponse(heartbeat=0.01)
    await ws.prepare(req)
    transport = req.transport
    assert transport is not None
    transport.is_closing.return_value = False  # type: ignore[attr-defined]

    # Transport EOF: web_protocol calls _cancel() from connection_lost.
    ws._cancel(ConnectionResetError("Connection lost"))
    # The pong timeout callback was already queued by the event loop.
    ws._pong_not_received()

    assert ws.closed
    assert ws.close_code == WSCloseCode.ABNORMAL_CLOSURE
    _assert_heartbeat_reclaimed(ws)

    # A close() after the fact must not emit anything on the wire.
    await ws.close()
    assert _close_frames_written(transport) == []
    assert len(transport.close.mock_calls) == 1  # type: ignore[attr-defined]


async def test_heartbeat_single_ping_timer_fake_loop() -> None:
    """One ping timer and one pong timeout per connection, fake loop."""
    ws = web.WebSocketResponse(heartbeat=10.0)
    fake_loop = mock.MagicMock()
    fake_loop.time.return_value = 100.0
    ws._loop = fake_loop

    ws._reset_heartbeat()
    assert fake_loop.call_at.call_count == 1
    ping_timer = ws._heartbeat_cb
    assert ping_timer is not None

    # Receiving data coalesces into a single reset and never creates
    # a second ping timer or a pong timeout.
    ws._on_data_received()
    ws._on_data_received()
    assert fake_loop.call_soon.call_count == 1
    ws._flush_heartbeat_reset()
    assert fake_loop.call_at.call_count == 1
    assert ws._heartbeat_cb is ping_timer
    assert ws._pong_response_cb is None

    # A pending pong timeout is cancelled when new data arrives.
    pong_timeout = mock.MagicMock()
    ws._pong_response_cb = pong_timeout
    ws._reset_heartbeat()
    pong_timeout.cancel.assert_called_once()
    assert ws._pong_response_cb is None

    # Shutdown reclaims the ping timer.
    ws._cancel_heartbeat()
    ping_timer.cancel.assert_called_once()
    _assert_heartbeat_reclaimed(ws)


async def test_heartbeat_disabled_no_timers(make_request: _RequestMaker) -> None:
    """With heartbeat=None no ping timer or pong timeout is ever created."""
    req = make_request("GET", "/")
    ws = web.WebSocketResponse(heartbeat=None)
    await ws.prepare(req)

    ws._on_data_received()
    await asyncio.sleep(0)

    assert ws._heartbeat_cb is None
    assert ws._pong_response_cb is None
    assert ws._heartbeat_reset_handle is None

    assert ws._reader is not None
    ws._reader.feed_data(WSMessageClose(data=1000, size=0, extra="close"))
    await ws.close()

    assert ws.closed
    _assert_heartbeat_reclaimed(ws)


async def test_data_received_after_close_does_not_resurrect_heartbeat(
    make_request: _RequestMaker,
) -> None:
    """Late frames after close must not reschedule the heartbeat."""
    req = make_request("GET", "/")
    ws = web.WebSocketResponse(heartbeat=0.01)
    await ws.prepare(req)
    transport = req.transport
    assert transport is not None
    transport.is_closing.return_value = False  # type: ignore[attr-defined]

    assert ws._reader is not None
    ws._reader.feed_data(WSMessageClose(data=1000, size=0, extra="close"))
    await ws.close()
    assert ws.closed

    ws._on_data_received()
    await asyncio.sleep(0)

    _assert_heartbeat_reclaimed(ws)


async def test_compressed_frame_then_close_single_close_frame(
    make_request: _RequestMaker,
) -> None:
    """Compressed frames followed by close: exactly one close frame."""
    headers = CIMultiDict(
        {
            "HOST": "server.example.com",
            "UPGRADE": "websocket",
            "CONNECTION": "Upgrade",
            "SEC-WEBSOCKET-KEY": "dGhlIHNhbXBsZSBub25jZQ==",
            "SEC-WEBSOCKET-VERSION": "13",
            "SEC-WEBSOCKET-EXTENSIONS": "permessage-deflate",
        }
    )
    req = make_request("GET", "/", headers=headers)
    ws = web.WebSocketResponse(heartbeat=0.01)
    await ws.prepare(req)
    assert ws.compress == 15
    transport = req.transport
    assert transport is not None
    transport.is_closing.return_value = False  # type: ignore[attr-defined]

    await ws.send_str("compressed payload")

    assert ws._reader is not None
    ws._reader.feed_data(WSMessageClose(data=1000, size=0, extra="close"))
    await ws.close()

    written = [call[0][0] for call in transport.write.call_args_list]  # type: ignore[attr-defined]
    # The compressed text frame (FIN|RSV1|TEXT) went out before the close.
    assert written[0][:1] == b"\xc1"
    assert _close_frames_written(transport) == [b"\x88\x02\x03\xe8"]
    assert ws.closed
    _assert_heartbeat_reclaimed(ws)


async def test_fragmented_message_then_close_does_not_resurrect(
    make_request: _RequestMaker,
) -> None:
    """Fragmented messages then close: the closed state is terminal."""
    req = make_request("GET", "/")
    ws = web.WebSocketResponse()
    await ws.prepare(req)
    transport = req.transport
    assert transport is not None
    transport.is_closing.return_value = False  # type: ignore[attr-defined]

    assert ws._reader is not None
    ws._reader.feed_data(WSMessageContinuation(data=b"hel", size=3))
    ws._reader.feed_data(WSMessageContinuation(data=b"lo", size=2))
    ws._reader.feed_data(WSMessageClose(data=1000, size=0, extra="close"))

    msg = await ws.receive()
    assert msg.type == WSMsgType.CONTINUATION
    msg = await ws.receive()
    assert msg.type == WSMsgType.CONTINUATION

    await ws.close()
    assert ws.closed
    assert ws.close_code == 1000
    assert len(_close_frames_written(transport)) == 1

    # The closed connection cannot be resurrected.
    assert (await ws.receive()) is WS_CLOSED_MESSAGE
    with pytest.raises(ConnectionError):
        await ws.send_str("x")
    assert len(_close_frames_written(transport)) == 1


async def test_iterator_early_exit_then_clean_close(
    make_request: _RequestMaker,
) -> None:
    """Breaking out of async iteration must not break the close handshake."""
    req = make_request("GET", "/")
    ws = web.WebSocketResponse(heartbeat=0.01)
    await ws.prepare(req)
    transport = req.transport
    assert transport is not None
    transport.is_closing.return_value = False  # type: ignore[attr-defined]

    assert ws._reader is not None
    ws._reader.feed_data(WSMessageText(data="hello", size=5))
    ws._reader.feed_data(WSMessageClose(data=1000, size=0, extra="close"))

    async for msg in ws:
        assert msg.type == WSMsgType.TEXT
        break  # application stops iterating early

    await ws.close()
    assert ws.closed
    assert ws.close_code == 1000
    assert _close_frames_written(transport) == [b"\x88\x02\x03\xe8"]
    _assert_heartbeat_reclaimed(ws)

    # The closed connection cannot be resurrected.
    assert (await ws.receive()) is WS_CLOSED_MESSAGE


async def test_close_cancellation_keeps_single_terminal_state(
    make_request: _RequestMaker,
) -> None:
    """Cancelling close() still leaves a single, reclaimable terminal state."""
    req = make_request("GET", "/")
    ws = web.WebSocketResponse(heartbeat=0.01)
    await ws.prepare(req)
    transport = req.transport
    assert transport is not None
    transport.is_closing.return_value = False  # type: ignore[attr-defined]

    ws._writer = mock.Mock()
    ws._writer.close = mock.AsyncMock(side_effect=asyncio.CancelledError())
    with pytest.raises(asyncio.CancelledError):
        await ws.close()

    assert ws.closed
    assert ws.close_code == WSCloseCode.ABNORMAL_CLOSURE
    assert len(transport.close.mock_calls) == 1  # type: ignore[attr-defined]
    _assert_heartbeat_reclaimed(ws)

    # A retry after the cancellation must not send another close frame.
    await ws.close()
    assert _close_frames_written(transport) == []


async def test_websocket_error_maps_to_close_frame_code(
    make_request: _RequestMaker,
) -> None:
    """A protocol error maps to the matching close code on the wire."""
    req = make_request("GET", "/")
    ws = web.WebSocketResponse()
    await ws.prepare(req)
    transport = req.transport
    assert transport is not None
    transport.is_closing.return_value = False  # type: ignore[attr-defined]

    exc = WebSocketError(WSCloseCode.PROTOCOL_ERROR, "bad frame")
    ws._reader = mock.Mock()
    ws._reader.read = mock.AsyncMock(side_effect=exc)

    msg = await ws.receive()
    assert msg.type == WSMsgType.ERROR
    assert msg.data is exc
    assert ws.closed

    close_frames = _close_frames_written(transport)
    assert close_frames == [b"\x88\x02\x03\xea"]  # 1002
