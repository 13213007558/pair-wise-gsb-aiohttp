import asyncio
import base64
import hashlib
import os
import random
from collections.abc import Mapping
from unittest import mock

import pytest

import aiohttp
from aiohttp import (
    ClientConnectionResetError,
    ClientWSTimeout,
    ServerDisconnectedError,
    ServerTimeoutError,
    WSCloseCode,
    client,
    hdrs,
)
from aiohttp._websocket.writer import WebSocketWriter as RealWebSocketWriter
from aiohttp.http import WS_CLOSED_MESSAGE, WS_KEY
from aiohttp.http_websocket import WSMessageClose, WSMessageText
from aiohttp.streams import EofStream


async def test_ws_connect(ws_key: str, key_data: bytes) -> None:
    resp = mock.Mock()
    resp.status = 101
    resp.headers = {
        hdrs.UPGRADE: "websocket",
        hdrs.CONNECTION: "upgrade",
        hdrs.SEC_WEBSOCKET_ACCEPT: ws_key,
        hdrs.SEC_WEBSOCKET_PROTOCOL: "chat",
    }
    resp._upgraded = True
    resp.connection.protocol.read_timeout = None
    with mock.patch("aiohttp.client.os") as m_os:
        with mock.patch("aiohttp.client.ClientSession.request") as m_req:
            m_os.urandom.return_value = key_data
            m_req.return_value = asyncio.get_running_loop().create_future()
            m_req.return_value.set_result(resp)

            res = await aiohttp.ClientSession().ws_connect(
                "http://test.org", protocols=("t1", "t2", "chat")
            )

    assert isinstance(res, client.ClientWebSocketResponse)
    assert res.protocol == "chat"
    assert hdrs.ORIGIN not in m_req.call_args[1]["headers"]


async def test_ws_connect_read_timeout_is_reset_to_inf(
    ws_key: str, key_data: bytes
) -> None:
    resp = mock.Mock()
    resp.status = 101
    resp.headers = {
        hdrs.UPGRADE: "websocket",
        hdrs.CONNECTION: "upgrade",
        hdrs.SEC_WEBSOCKET_ACCEPT: ws_key,
        hdrs.SEC_WEBSOCKET_PROTOCOL: "chat",
    }
    resp.connection.protocol.read_timeout = 0.5
    with (
        mock.patch("aiohttp.client.os") as m_os,
        mock.patch("aiohttp.client.ClientSession.request") as m_req,
    ):
        m_os.urandom.return_value = key_data
        m_req.return_value = asyncio.get_running_loop().create_future()
        m_req.return_value.set_result(resp)

        res = await aiohttp.ClientSession().ws_connect(
            "http://test.org", protocols=("t1", "t2", "chat")
        )

    assert isinstance(res, client.ClientWebSocketResponse)
    assert res.protocol == "chat"
    assert hdrs.ORIGIN not in m_req.call_args[1]["headers"]
    assert resp.connection.protocol.read_timeout is None


async def test_ws_connect_read_timeout_stays_inf(ws_key: str, key_data: bytes) -> None:
    resp = mock.Mock()
    resp.status = 101
    resp.headers = {
        hdrs.UPGRADE: "websocket",
        hdrs.CONNECTION: "upgrade",
        hdrs.SEC_WEBSOCKET_ACCEPT: ws_key,
        hdrs.SEC_WEBSOCKET_PROTOCOL: "chat",
    }
    resp.connection.protocol.read_timeout = None
    with (
        mock.patch("aiohttp.client.os") as m_os,
        mock.patch("aiohttp.client.ClientSession.request") as m_req,
    ):
        m_os.urandom.return_value = key_data
        m_req.return_value = asyncio.get_running_loop().create_future()
        m_req.return_value.set_result(resp)

        res = await aiohttp.ClientSession().ws_connect(
            "http://test.org",
            protocols=("t1", "t2", "chat"),
            timeout=ClientWSTimeout(0.5),
        )

    assert isinstance(res, client.ClientWebSocketResponse)
    assert res.protocol == "chat"
    assert hdrs.ORIGIN not in m_req.call_args[1]["headers"]
    assert resp.connection.protocol.read_timeout is None


async def test_ws_connect_read_timeout_reset_to_max(
    ws_key: str, key_data: bytes
) -> None:
    resp = mock.Mock()
    resp.status = 101
    resp.headers = {
        hdrs.UPGRADE: "websocket",
        hdrs.CONNECTION: "upgrade",
        hdrs.SEC_WEBSOCKET_ACCEPT: ws_key,
        hdrs.SEC_WEBSOCKET_PROTOCOL: "chat",
    }
    resp.connection.protocol.read_timeout = 0.5
    with (
        mock.patch("aiohttp.client.os") as m_os,
        mock.patch("aiohttp.client.ClientSession.request") as m_req,
    ):
        m_os.urandom.return_value = key_data
        m_req.return_value = asyncio.get_running_loop().create_future()
        m_req.return_value.set_result(resp)

        res = await aiohttp.ClientSession().ws_connect(
            "http://test.org",
            protocols=("t1", "t2", "chat"),
            timeout=ClientWSTimeout(1.0),
        )

    assert isinstance(res, client.ClientWebSocketResponse)
    assert res.protocol == "chat"
    assert hdrs.ORIGIN not in m_req.call_args[1]["headers"]
    assert resp.connection.protocol.read_timeout == 1.0


async def test_ws_connect_with_origin(key_data: bytes) -> None:
    resp = mock.Mock()
    resp.status = 403
    with mock.patch("aiohttp.client.os") as m_os:
        with mock.patch("aiohttp.client.ClientSession.request") as m_req:
            m_os.urandom.return_value = key_data
            m_req.return_value = asyncio.get_running_loop().create_future()
            m_req.return_value.set_result(resp)

            origin = "https://example.org/page.html"
            with pytest.raises(client.WSServerHandshakeError):
                await aiohttp.ClientSession().ws_connect(
                    "http://test.org", origin=origin
                )

    assert hdrs.ORIGIN in m_req.call_args[1]["headers"]
    assert m_req.call_args[1]["headers"][hdrs.ORIGIN] == origin


async def test_ws_connect_with_params(ws_key: str, key_data: bytes) -> None:
    params = {"key1": "value1", "key2": "value2"}

    resp = mock.Mock()
    resp.status = 101
    resp.headers = {
        hdrs.UPGRADE: "websocket",
        hdrs.CONNECTION: "upgrade",
        hdrs.SEC_WEBSOCKET_ACCEPT: ws_key,
        hdrs.SEC_WEBSOCKET_PROTOCOL: "chat",
    }
    resp.connection.protocol.read_timeout = None
    with mock.patch("aiohttp.client.os") as m_os:
        with mock.patch("aiohttp.client.ClientSession.request") as m_req:
            m_os.urandom.return_value = key_data
            m_req.return_value = asyncio.get_running_loop().create_future()
            m_req.return_value.set_result(resp)

            await aiohttp.ClientSession().ws_connect(
                "http://test.org", protocols=("t1", "t2", "chat"), params=params
            )

    assert m_req.call_args[1]["params"] == params


async def test_ws_connect_custom_response(ws_key: str, key_data: bytes) -> None:
    class CustomResponse(client.ClientWebSocketResponse):
        def read(self, decode: bool = False) -> str:
            return "customized!"

    resp = mock.Mock()
    resp.status = 101
    resp.headers = {
        hdrs.UPGRADE: "websocket",
        hdrs.CONNECTION: "upgrade",
        hdrs.SEC_WEBSOCKET_ACCEPT: ws_key,
    }
    resp.connection.protocol.read_timeout = None
    with mock.patch("aiohttp.client.os") as m_os:
        with mock.patch("aiohttp.client.ClientSession.request") as m_req:
            m_os.urandom.return_value = key_data
            m_req.return_value = asyncio.get_running_loop().create_future()
            m_req.return_value.set_result(resp)

            res = await aiohttp.ClientSession(
                ws_response_class=CustomResponse
            ).ws_connect("http://test.org")

    # TODO(PY313): Use TypeVar(default=) to make ClientSession Generic over response classes.
    #              Then .ws_connect() can return CustomResponse here.
    assert res.read() == "customized!"  # type: ignore[attr-defined]


async def test_ws_connect_err_status(ws_key: str, key_data: bytes) -> None:
    resp = mock.Mock()
    resp.status = 500
    resp.headers = {
        hdrs.UPGRADE: "websocket",
        hdrs.CONNECTION: "upgrade",
        hdrs.SEC_WEBSOCKET_ACCEPT: ws_key,
    }
    with mock.patch("aiohttp.client.os") as m_os:
        with mock.patch("aiohttp.client.ClientSession.request") as m_req:
            m_os.urandom.return_value = key_data
            m_req.return_value = asyncio.get_running_loop().create_future()
            m_req.return_value.set_result(resp)

            with pytest.raises(client.WSServerHandshakeError) as ctx:
                await aiohttp.ClientSession().ws_connect(
                    "http://test.org", protocols=("t1", "t2", "chat")
                )

    assert ctx.value.message == "Invalid response status"


async def test_ws_connect_err_upgrade(ws_key: str, key_data: bytes) -> None:
    resp = mock.Mock()
    resp.status = 101
    resp.headers = {
        hdrs.UPGRADE: "test",
        hdrs.CONNECTION: "upgrade",
        hdrs.SEC_WEBSOCKET_ACCEPT: ws_key,
    }
    with mock.patch("aiohttp.client.os") as m_os:
        with mock.patch("aiohttp.client.ClientSession.request") as m_req:
            m_os.urandom.return_value = key_data
            m_req.return_value = asyncio.get_running_loop().create_future()
            m_req.return_value.set_result(resp)

            with pytest.raises(client.WSServerHandshakeError) as ctx:
                await aiohttp.ClientSession().ws_connect(
                    "http://test.org", protocols=("t1", "t2", "chat")
                )

    assert ctx.value.message == "Invalid upgrade header"


async def test_ws_connect_err_conn(ws_key: str, key_data: bytes) -> None:
    # The parser did not see a Connection: upgrade token (resp._upgraded is
    # False), so the handshake must be rejected.
    resp = mock.Mock()
    resp.status = 101
    resp.headers = {
        hdrs.UPGRADE: "websocket",
        hdrs.CONNECTION: "close",
        hdrs.SEC_WEBSOCKET_ACCEPT: ws_key,
    }
    resp._upgraded = False
    with mock.patch("aiohttp.client.os") as m_os:
        with mock.patch("aiohttp.client.ClientSession.request") as m_req:
            m_os.urandom.return_value = key_data
            m_req.return_value = asyncio.get_running_loop().create_future()
            m_req.return_value.set_result(resp)

            with pytest.raises(client.WSServerHandshakeError) as ctx:
                await aiohttp.ClientSession().ws_connect(
                    "http://test.org", protocols=("t1", "t2", "chat")
                )

    assert ctx.value.message == "Invalid connection header"


async def test_ws_connect_err_challenge(ws_key: str, key_data: bytes) -> None:
    resp = mock.Mock()
    resp.status = 101
    resp.headers = {
        hdrs.UPGRADE: "websocket",
        hdrs.CONNECTION: "upgrade",
        hdrs.SEC_WEBSOCKET_ACCEPT: "asdfasdfasdfasdfasdfasdf",
    }
    with mock.patch("aiohttp.client.os") as m_os:
        with mock.patch("aiohttp.client.ClientSession.request") as m_req:
            m_os.urandom.return_value = key_data
            m_req.return_value = asyncio.get_running_loop().create_future()
            m_req.return_value.set_result(resp)

            with pytest.raises(client.WSServerHandshakeError) as ctx:
                await aiohttp.ClientSession().ws_connect(
                    "http://test.org", protocols=("t1", "t2", "chat")
                )

    assert ctx.value.message == "Invalid challenge response"


async def test_ws_connect_common_headers(ws_key: str, key_data: bytes) -> None:
    # Emulate a headers dict being reused for a second ws_connect.

    # In this scenario, we need to ensure that the newly generated secret key
    # is sent to the server, not the stale key.
    headers: dict[str, str] = {}

    async def test_connection() -> None:
        async def mock_get(
            *args: object, headers: Mapping[str, str], **kwargs: object
        ) -> mock.Mock:
            resp = mock.Mock()
            resp.status = 101
            key = headers[hdrs.SEC_WEBSOCKET_KEY]
            accept = base64.b64encode(
                hashlib.sha1(base64.b64encode(base64.b64decode(key)) + WS_KEY).digest()
            ).decode()
            resp.headers = {
                hdrs.UPGRADE: "websocket",
                hdrs.CONNECTION: "upgrade",
                hdrs.SEC_WEBSOCKET_ACCEPT: accept,
                hdrs.SEC_WEBSOCKET_PROTOCOL: "chat",
            }
            resp.connection.protocol.read_timeout = None
            return resp

        with mock.patch("aiohttp.client.os") as m_os:
            with mock.patch(
                "aiohttp.client.ClientSession.request", side_effect=mock_get
            ) as m_req:
                m_os.urandom.return_value = key_data

                res = await aiohttp.ClientSession().ws_connect(
                    "http://test.org", protocols=("t1", "t2", "chat"), headers=headers
                )

        assert isinstance(res, client.ClientWebSocketResponse)
        assert res.protocol == "chat"
        assert hdrs.ORIGIN not in m_req.call_args[1]["headers"]

    await test_connection()
    # Generate a new ws key
    key_data = os.urandom(16)
    await test_connection()


async def test_close(ws_key: str, key_data: bytes) -> None:
    mresp = mock.Mock()
    mresp.status = 101
    mresp.headers = {
        hdrs.UPGRADE: "websocket",
        hdrs.CONNECTION: "upgrade",
        hdrs.SEC_WEBSOCKET_ACCEPT: ws_key,
    }
    mresp.connection.protocol.read_timeout = None
    with mock.patch("aiohttp.client.WebSocketWriter") as WebSocketWriter:
        with mock.patch("aiohttp.client.os") as m_os:
            with mock.patch("aiohttp.client.ClientSession.request") as m_req:
                m_os.urandom.return_value = key_data
                m_req.return_value = asyncio.get_running_loop().create_future()
                m_req.return_value.set_result(mresp)
                writer = mock.create_autospec(
                    RealWebSocketWriter, instance=True, spec_set=True
                )
                WebSocketWriter.return_value = writer

                session = aiohttp.ClientSession()
                resp = await session.ws_connect("http://test.org")
                assert not resp.closed

                resp._reader.feed_data(WSMessageClose(data=0, size=0, extra=""))

                res = await resp.close()
                writer.close.assert_called_with(1000, b"")
                assert resp.closed
                assert res  # type: ignore[unreachable]
                assert resp.exception() is None

                # idempotent
                res = await resp.close()
                assert not res
                assert writer.close.call_count == 1

                await session.close()


async def test_close_eofstream(ws_key: str, key_data: bytes) -> None:
    mresp = mock.Mock()
    mresp.status = 101
    mresp.headers = {
        hdrs.UPGRADE: "websocket",
        hdrs.CONNECTION: "upgrade",
        hdrs.SEC_WEBSOCKET_ACCEPT: ws_key,
    }
    mresp.connection.protocol.read_timeout = None
    with mock.patch("aiohttp.client.WebSocketWriter") as WebSocketWriter:
        with mock.patch("aiohttp.client.os") as m_os:
            with mock.patch("aiohttp.client.ClientSession.request") as m_req:
                m_os.urandom.return_value = key_data
                m_req.return_value = asyncio.get_running_loop().create_future()
                m_req.return_value.set_result(mresp)
                writer = WebSocketWriter.return_value = mock.Mock()

                session = aiohttp.ClientSession()
                resp = await session.ws_connect("http://test.org")
                assert not resp.closed

                exc = EofStream()
                resp._reader.set_exception(exc)

                await resp.receive()
                writer.close.assert_called_with(1000, b"")
                assert resp.closed

                await session.close()  # type: ignore[unreachable]


async def test_close_connection_lost(ws_key: str, key_data: bytes) -> None:
    """Test the websocket client handles the connection being closed out from under it."""
    mresp = mock.Mock(spec_set=client.ClientResponse)
    mresp.status = 101
    mresp.headers = {
        hdrs.UPGRADE: "websocket",
        hdrs.CONNECTION: "upgrade",
        hdrs.SEC_WEBSOCKET_ACCEPT: ws_key,
    }
    mresp.connection.protocol.read_timeout = None
    with (
        mock.patch("aiohttp.client.WebSocketWriter"),
        mock.patch("aiohttp.client.os") as m_os,
        mock.patch("aiohttp.client.ClientSession.request") as m_req,
    ):
        m_os.urandom.return_value = key_data
        m_req.return_value = asyncio.get_running_loop().create_future()
        m_req.return_value.set_result(mresp)

        session = aiohttp.ClientSession()
        resp = await session.ws_connect("http://test.org")
        assert not resp.closed

        exc = ServerDisconnectedError()
        resp._reader.set_exception(exc)

        msg = await resp.receive()
        assert msg.type is aiohttp.WSMsgType.CLOSED
        assert resp.closed

        await session.close()  # type: ignore[unreachable]


async def test_close_exc(ws_key: str, key_data: bytes) -> None:
    mresp = mock.Mock()
    mresp.status = 101
    mresp.headers = {
        hdrs.UPGRADE: "websocket",
        hdrs.CONNECTION: "upgrade",
        hdrs.SEC_WEBSOCKET_ACCEPT: ws_key,
    }
    mresp.connection.protocol.read_timeout = None
    with mock.patch("aiohttp.client.WebSocketWriter") as WebSocketWriter:
        with mock.patch("aiohttp.client.os") as m_os:
            with mock.patch("aiohttp.client.ClientSession.request") as m_req:
                m_os.urandom.return_value = key_data
                m_req.return_value = asyncio.get_running_loop().create_future()
                m_req.return_value.set_result(mresp)
                writer = mock.create_autospec(
                    RealWebSocketWriter, instance=True, spec_set=True
                )
                WebSocketWriter.return_value = writer

                session = aiohttp.ClientSession()
                resp = await session.ws_connect("http://test.org")
                assert not resp.closed

                exc = ValueError()
                resp._reader.set_exception(exc)

                await resp.close()
                assert resp.closed
                assert resp.exception() is exc  # type: ignore[unreachable]

                await session.close()


async def test_close_exc2(ws_key: str, key_data: bytes) -> None:
    mresp = mock.Mock()
    mresp.status = 101
    mresp.headers = {
        hdrs.UPGRADE: "websocket",
        hdrs.CONNECTION: "upgrade",
        hdrs.SEC_WEBSOCKET_ACCEPT: ws_key,
    }
    mresp.connection.protocol.read_timeout = None
    with mock.patch("aiohttp.client.WebSocketWriter") as WebSocketWriter:
        with mock.patch("aiohttp.client.os") as m_os:
            with mock.patch("aiohttp.client.ClientSession.request") as m_req:
                m_os.urandom.return_value = key_data
                m_req.return_value = asyncio.get_running_loop().create_future()
                m_req.return_value.set_result(mresp)
                writer = WebSocketWriter.return_value = mock.Mock()

                resp = await aiohttp.ClientSession().ws_connect("http://test.org")
                assert not resp.closed

                exc = ValueError()
                writer.close.side_effect = exc

                await resp.close()
                assert resp.closed
                assert resp.exception() is exc  # type: ignore[unreachable]

                resp._closed = False
                writer.close.side_effect = asyncio.CancelledError()
                with pytest.raises(asyncio.CancelledError):
                    await resp.close()


@pytest.mark.parametrize("exc", (ClientConnectionResetError, ConnectionResetError))
async def test_send_data_after_close(
    exc: type[Exception], ws_key: str, key_data: bytes
) -> None:
    mresp = mock.Mock()
    mresp.status = 101
    mresp.headers = {
        hdrs.UPGRADE: "websocket",
        hdrs.CONNECTION: "upgrade",
        hdrs.SEC_WEBSOCKET_ACCEPT: ws_key,
    }
    mresp.connection.protocol.read_timeout = None
    with mock.patch("aiohttp.client.os") as m_os:
        with mock.patch("aiohttp.client.ClientSession.request") as m_req:
            m_os.urandom.return_value = key_data
            m_req.return_value = asyncio.get_running_loop().create_future()
            m_req.return_value.set_result(mresp)

            resp = await aiohttp.ClientSession().ws_connect("http://test.org")
            resp._writer._closing = True

            for meth, args in (
                (resp.ping, ()),
                (resp.pong, ()),
                (resp.send_str, ("s",)),
                (resp.send_bytes, (b"b",)),
                (resp.send_json, ({},)),
                (resp.send_frame, (b"", aiohttp.WSMsgType.BINARY)),
            ):
                with pytest.raises(exc):  # Verify exc can be caught with both classes
                    await meth(*args)


async def test_send_data_type_errors(ws_key: str, key_data: bytes) -> None:
    mresp = mock.Mock()
    mresp.status = 101
    mresp.headers = {
        hdrs.UPGRADE: "websocket",
        hdrs.CONNECTION: "upgrade",
        hdrs.SEC_WEBSOCKET_ACCEPT: ws_key,
    }
    mresp.connection.protocol.read_timeout = None
    with mock.patch("aiohttp.client.WebSocketWriter") as WebSocketWriter:
        with mock.patch("aiohttp.client.os") as m_os:
            with mock.patch("aiohttp.client.ClientSession.request") as m_req:
                m_os.urandom.return_value = key_data
                m_req.return_value = asyncio.get_running_loop().create_future()
                m_req.return_value.set_result(mresp)
                WebSocketWriter.return_value = mock.Mock()

                resp = await aiohttp.ClientSession().ws_connect("http://test.org")

                with pytest.raises(TypeError):
                    await resp.send_str(b"s")  # type: ignore[arg-type]
                with pytest.raises(TypeError):
                    await resp.send_bytes("b")  # type: ignore[arg-type]
                with pytest.raises(TypeError):
                    await resp.send_json(set())


async def test_reader_read_exception(ws_key: str, key_data: bytes) -> None:
    hresp = mock.Mock()
    hresp.status = 101
    hresp.headers = {
        hdrs.UPGRADE: "websocket",
        hdrs.CONNECTION: "upgrade",
        hdrs.SEC_WEBSOCKET_ACCEPT: ws_key,
    }
    hresp.connection.protocol.read_timeout = None
    with mock.patch("aiohttp.client.WebSocketWriter") as WebSocketWriter:
        with mock.patch("aiohttp.client.os") as m_os:
            with mock.patch("aiohttp.client.ClientSession.request") as m_req:
                m_os.urandom.return_value = key_data
                m_req.return_value = asyncio.get_running_loop().create_future()
                m_req.return_value.set_result(hresp)

                writer = mock.create_autospec(
                    RealWebSocketWriter, instance=True, spec_set=True
                )
                WebSocketWriter.return_value = writer

                session = aiohttp.ClientSession()
                resp = await session.ws_connect("http://test.org")

                exc = ValueError()
                resp._reader.set_exception(exc)

                msg = await resp.receive()
                assert msg.type == aiohttp.WSMsgType.ERROR
                assert resp.exception() is exc

                await session.close()


async def test_receive_runtime_err() -> None:
    resp = client.ClientWebSocketResponse(
        mock.Mock(),
        mock.Mock(),
        mock.Mock(),
        mock.Mock(),
        ClientWSTimeout(ws_receive=10.0),
        True,
        True,
        asyncio.get_running_loop(),
    )
    resp._waiting = True

    with pytest.raises(RuntimeError):
        await resp.receive()


async def test_heartbeat_reset_coalesces_on_data() -> None:
    response = mock.Mock()
    response.connection = None
    resp = client.ClientWebSocketResponse(
        mock.Mock(),
        mock.Mock(),
        None,
        response,
        ClientWSTimeout(ws_receive=10.0),
        True,
        True,
        asyncio.get_running_loop(),
        heartbeat=0.05,
    )
    with mock.patch.object(resp, "_reset_heartbeat", autospec=True) as reset:
        resp._on_data_received()
        resp._on_data_received()

        await asyncio.sleep(0)

        assert reset.call_count == 1


async def test_receive_does_not_reset_heartbeat() -> None:
    response = mock.Mock()
    response.connection = None
    msg = mock.Mock(type=aiohttp.WSMsgType.TEXT)
    reader = mock.Mock()
    reader.read = mock.AsyncMock(return_value=msg)
    resp = client.ClientWebSocketResponse(
        reader,
        mock.Mock(),
        None,
        response,
        ClientWSTimeout(ws_receive=10.0),
        True,
        True,
        asyncio.get_running_loop(),
        heartbeat=0.05,
    )
    with mock.patch.object(resp, "_reset_heartbeat", autospec=True) as reset:
        received = await resp.receive()

    assert received is msg
    reset.assert_not_called()


async def test_cancel_heartbeat_cancels_pending_heartbeat_reset_handle() -> None:
    response = mock.Mock()
    response.connection = None
    resp = client.ClientWebSocketResponse(
        mock.Mock(),
        mock.Mock(),
        None,
        response,
        ClientWSTimeout(ws_receive=10.0),
        True,
        True,
        asyncio.get_running_loop(),
        heartbeat=0.05,
    )

    resp._on_data_received()
    handle = resp._heartbeat_reset_handle
    assert handle is not None

    resp._cancel_heartbeat()

    assert resp._heartbeat_reset_handle is None
    assert resp._need_heartbeat_reset is False
    assert handle.cancelled()


async def test_flush_heartbeat_reset_returns_early_when_not_needed() -> None:
    response = mock.Mock()
    response.connection = None
    resp = client.ClientWebSocketResponse(
        mock.Mock(),
        mock.Mock(),
        None,
        response,
        ClientWSTimeout(ws_receive=10.0),
        True,
        True,
        asyncio.get_running_loop(),
        heartbeat=0.05,
    )
    resp._need_heartbeat_reset = False

    with mock.patch.object(resp, "_reset_heartbeat", autospec=True) as reset:
        resp._flush_heartbeat_reset()
        reset.assert_not_called()


async def test_send_heartbeat_returns_early_when_reset_is_pending() -> None:
    response = mock.Mock()
    response.connection = None
    writer = mock.Mock()
    resp = client.ClientWebSocketResponse(
        mock.Mock(),
        writer,
        None,
        response,
        ClientWSTimeout(ws_receive=10.0),
        True,
        True,
        asyncio.get_running_loop(),
        heartbeat=0.05,
    )
    resp._need_heartbeat_reset = True

    resp._send_heartbeat()

    writer.send_frame.assert_not_called()


async def test_ws_connect_close_resp_on_err(ws_key: str, key_data: bytes) -> None:
    resp = mock.Mock()
    resp.status = 500
    resp.headers = {
        hdrs.UPGRADE: "websocket",
        hdrs.CONNECTION: "upgrade",
        hdrs.SEC_WEBSOCKET_ACCEPT: ws_key,
    }
    with mock.patch("aiohttp.client.os") as m_os:
        with mock.patch("aiohttp.client.ClientSession.request") as m_req:
            m_os.urandom.return_value = key_data
            m_req.return_value = asyncio.get_running_loop().create_future()
            m_req.return_value.set_result(resp)

            with pytest.raises(client.WSServerHandshakeError):
                await aiohttp.ClientSession().ws_connect(
                    "http://test.org", protocols=("t1", "t2", "chat")
                )
            resp.close.assert_called_with()


async def test_ws_connect_non_overlapped_protocols(
    ws_key: str, key_data: bytes
) -> None:
    resp = mock.Mock()
    resp.status = 101
    resp.headers = {
        hdrs.UPGRADE: "websocket",
        hdrs.CONNECTION: "upgrade",
        hdrs.SEC_WEBSOCKET_ACCEPT: ws_key,
        hdrs.SEC_WEBSOCKET_PROTOCOL: "other,another",
    }
    resp.connection.protocol.read_timeout = None
    with mock.patch("aiohttp.client.os") as m_os:
        with mock.patch("aiohttp.client.ClientSession.request") as m_req:
            m_os.urandom.return_value = key_data
            m_req.return_value = asyncio.get_running_loop().create_future()
            m_req.return_value.set_result(resp)

            res = await aiohttp.ClientSession().ws_connect(
                "http://test.org", protocols=("t1", "t2", "chat")
            )

    assert res.protocol is None


async def test_ws_connect_non_overlapped_protocols_2(
    ws_key: str, key_data: bytes
) -> None:
    resp = mock.Mock()
    resp.status = 101
    resp.headers = {
        hdrs.UPGRADE: "websocket",
        hdrs.CONNECTION: "upgrade",
        hdrs.SEC_WEBSOCKET_ACCEPT: ws_key,
        hdrs.SEC_WEBSOCKET_PROTOCOL: "other,another",
    }
    resp.connection.protocol.read_timeout = None
    with mock.patch("aiohttp.client.os") as m_os:
        with mock.patch("aiohttp.client.ClientSession.request") as m_req:
            m_os.urandom.return_value = key_data
            m_req.return_value = asyncio.get_running_loop().create_future()
            m_req.return_value.set_result(resp)

            connector = aiohttp.TCPConnector(force_close=True)
            res = await aiohttp.ClientSession(connector=connector).ws_connect(
                "http://test.org", protocols=("t1", "t2", "chat")
            )

    assert res.protocol is None
    del res


async def test_ws_connect_deflate(ws_key: str, key_data: bytes) -> None:
    resp = mock.Mock()
    resp.status = 101
    resp.headers = {
        hdrs.UPGRADE: "websocket",
        hdrs.CONNECTION: "upgrade",
        hdrs.SEC_WEBSOCKET_ACCEPT: ws_key,
        hdrs.SEC_WEBSOCKET_EXTENSIONS: "permessage-deflate",
    }
    resp.connection.protocol.read_timeout = None
    with mock.patch("aiohttp.client.os") as m_os:
        with mock.patch("aiohttp.client.ClientSession.request") as m_req:
            m_os.urandom.return_value = key_data
            m_req.return_value = asyncio.get_running_loop().create_future()
            m_req.return_value.set_result(resp)

            res = await aiohttp.ClientSession().ws_connect(
                "http://test.org", compress=15
            )

    assert res.compress == 15
    assert res.client_notakeover is False


async def test_ws_connect_deflate_per_message(ws_key: str, key_data: bytes) -> None:
    mresp = mock.Mock()
    mresp.status = 101
    mresp.headers = {
        hdrs.UPGRADE: "websocket",
        hdrs.CONNECTION: "upgrade",
        hdrs.SEC_WEBSOCKET_ACCEPT: ws_key,
        hdrs.SEC_WEBSOCKET_EXTENSIONS: "permessage-deflate",
    }
    mresp.connection.protocol.read_timeout = None
    with mock.patch("aiohttp.client.WebSocketWriter") as WebSocketWriter:
        with mock.patch("aiohttp.client.os") as m_os:
            with mock.patch("aiohttp.client.ClientSession.request") as m_req:
                m_os.urandom.return_value = key_data
                m_req.return_value = asyncio.get_running_loop().create_future()
                m_req.return_value.set_result(mresp)
                writer = mock.create_autospec(
                    RealWebSocketWriter, instance=True, spec_set=True
                )

                WebSocketWriter.return_value = writer

                session = aiohttp.ClientSession()
                resp = await session.ws_connect("http://test.org")

                await resp.send_str("string", compress=-1)
                writer.send_frame.assert_called_with(
                    b"string", aiohttp.WSMsgType.TEXT, compress=-1
                )

                await resp.send_bytes(b"bytes", compress=15)
                writer.send_frame.assert_called_with(
                    b"bytes", aiohttp.WSMsgType.BINARY, compress=15
                )

                await resp.send_json([{}], compress=-9)
                writer.send_frame.assert_called_with(
                    b"[{}]", aiohttp.WSMsgType.TEXT, compress=-9
                )

                await resp.send_frame(b"[{}]", aiohttp.WSMsgType.TEXT, compress=-9)
                writer.send_frame.assert_called_with(
                    b"[{}]", aiohttp.WSMsgType.TEXT, -9
                )

                await session.close()


async def test_ws_connect_deflate_server_not_support(
    ws_key: str, key_data: bytes
) -> None:
    resp = mock.Mock()
    resp.status = 101
    resp.headers = {
        hdrs.UPGRADE: "websocket",
        hdrs.CONNECTION: "upgrade",
        hdrs.SEC_WEBSOCKET_ACCEPT: ws_key,
    }
    resp.connection.protocol.read_timeout = None
    with mock.patch("aiohttp.client.os") as m_os:
        with mock.patch("aiohttp.client.ClientSession.request") as m_req:
            m_os.urandom.return_value = key_data
            m_req.return_value = asyncio.get_running_loop().create_future()
            m_req.return_value.set_result(resp)

            res = await aiohttp.ClientSession().ws_connect(
                "http://test.org", compress=15
            )

    assert res.compress == 0
    assert res.client_notakeover is False


async def test_ws_connect_deflate_notakeover(ws_key: str, key_data: bytes) -> None:
    resp = mock.Mock()
    resp.status = 101
    resp.headers = {
        hdrs.UPGRADE: "websocket",
        hdrs.CONNECTION: "upgrade",
        hdrs.SEC_WEBSOCKET_ACCEPT: ws_key,
        hdrs.SEC_WEBSOCKET_EXTENSIONS: "permessage-deflate; "
        "client_no_context_takeover",
    }
    resp.connection.protocol.read_timeout = None
    with mock.patch("aiohttp.client.os") as m_os:
        with mock.patch("aiohttp.client.ClientSession.request") as m_req:
            m_os.urandom.return_value = key_data
            m_req.return_value = asyncio.get_running_loop().create_future()
            m_req.return_value.set_result(resp)

            res = await aiohttp.ClientSession().ws_connect(
                "http://test.org", compress=15
            )

    assert res.compress == 15
    assert res.client_notakeover is True


async def test_ws_connect_deflate_client_wbits(ws_key: str, key_data: bytes) -> None:
    resp = mock.Mock()
    resp.status = 101
    resp.headers = {
        hdrs.UPGRADE: "websocket",
        hdrs.CONNECTION: "upgrade",
        hdrs.SEC_WEBSOCKET_ACCEPT: ws_key,
        hdrs.SEC_WEBSOCKET_EXTENSIONS: "permessage-deflate; "
        "client_max_window_bits=10",
    }
    resp.connection.protocol.read_timeout = None
    with mock.patch("aiohttp.client.os") as m_os:
        with mock.patch("aiohttp.client.ClientSession.request") as m_req:
            m_os.urandom.return_value = key_data
            m_req.return_value = asyncio.get_running_loop().create_future()
            m_req.return_value.set_result(resp)

            res = await aiohttp.ClientSession().ws_connect(
                "http://test.org", compress=15
            )

    assert res.compress == 10
    assert res.client_notakeover is False


async def test_ws_connect_deflate_client_wbits_bad(
    ws_key: str, key_data: bytes
) -> None:
    resp = mock.Mock()
    resp.status = 101
    resp.headers = {
        hdrs.UPGRADE: "websocket",
        hdrs.CONNECTION: "upgrade",
        hdrs.SEC_WEBSOCKET_ACCEPT: ws_key,
        hdrs.SEC_WEBSOCKET_EXTENSIONS: "permessage-deflate; "
        "client_max_window_bits=6",
    }
    with mock.patch("aiohttp.client.os") as m_os:
        with mock.patch("aiohttp.client.ClientSession.request") as m_req:
            m_os.urandom.return_value = key_data
            m_req.return_value = asyncio.get_running_loop().create_future()
            m_req.return_value.set_result(resp)

            with pytest.raises(client.WSServerHandshakeError):
                await aiohttp.ClientSession().ws_connect("http://test.org", compress=15)


def _make_client_ws(
    *,
    reader: object = None,
    writer: object = None,
    response: object = None,
    heartbeat: float | None = None,
    timeout: ClientWSTimeout | None = None,
    loop: object = None,
) -> client.ClientWebSocketResponse:
    """Create a ClientWebSocketResponse with mocked dependencies."""
    if response is None:
        response = mock.Mock()
        response.connection = None
    return client.ClientWebSocketResponse(
        reader if reader is not None else mock.Mock(),
        writer if writer is not None else mock.Mock(),
        None,
        response,  # type: ignore[arg-type]
        timeout if timeout is not None else ClientWSTimeout(ws_receive=10.0),
        True,
        True,
        loop if loop is not None else asyncio.get_running_loop(),  # type: ignore[arg-type]
        heartbeat=heartbeat,
    )


def _close_frames_written(transport: mock.Mock) -> list[bytes]:
    """Return the close frames written to the transport so far."""
    return [
        call[0][0]
        for call in transport.write.call_args_list
        if call[0][0][:1] == b"\x88"
    ]


def _assert_heartbeat_reclaimed(resp: client.ClientWebSocketResponse) -> None:
    """Assert no heartbeat timer or task is left behind."""
    assert resp._heartbeat_cb is None
    assert resp._pong_response_cb is None
    assert resp._ping_task is None
    assert resp._heartbeat_reset_handle is None
    assert resp._need_heartbeat_reset is False


async def test_pong_timeout_maps_to_server_timeout_error() -> None:
    """A pong timeout closes the connection with a stable exception mapping."""
    response = mock.Mock()
    response.connection = None
    resp = _make_client_ws(response=response, heartbeat=0.05)

    resp._pong_not_received()

    assert resp.closed
    assert resp._close_code == WSCloseCode.ABNORMAL_CLOSURE
    assert isinstance(resp.exception(), ServerTimeoutError)
    response.close.assert_called_once()
    _assert_heartbeat_reclaimed(resp)


async def test_close_sends_single_close_frame_and_reclaims_heartbeat() -> None:
    """close() emits exactly one close frame and reclaims the heartbeat."""
    response = mock.Mock()
    response.connection = None
    reader = mock.Mock()
    reader.read = mock.AsyncMock(
        return_value=WSMessageClose(data=1000, size=0, extra="close")
    )
    transport = mock.Mock()
    transport.is_closing.return_value = False
    writer = RealWebSocketWriter(
        mock.Mock(), transport, use_mask=True, random=random.Random(123)
    )
    resp = _make_client_ws(
        reader=reader,
        writer=writer,
        response=response,
        heartbeat=0.05,
        timeout=ClientWSTimeout(ws_receive=10.0, ws_close=10.0),
    )
    assert resp._heartbeat_cb is not None

    await resp.close()
    await resp.close()

    assert len(_close_frames_written(transport)) == 1
    assert resp._close_code == 1000
    response.close.assert_called_once()
    _assert_heartbeat_reclaimed(resp)


async def test_receive_cancelled_reclaims_heartbeat() -> None:
    """Cancelling receive() must not leak the heartbeat timers/task."""
    reader = mock.Mock()
    reader.read = mock.AsyncMock(side_effect=asyncio.CancelledError())
    resp = _make_client_ws(reader=reader, heartbeat=0.05)
    assert resp._heartbeat_cb is not None

    with pytest.raises(asyncio.CancelledError):
        await resp.receive()

    assert resp._close_code == WSCloseCode.ABNORMAL_CLOSURE
    _assert_heartbeat_reclaimed(resp)


async def test_heartbeat_disabled_no_timers() -> None:
    """With heartbeat=None no ping timer or pong timeout is ever created."""
    resp = _make_client_ws()

    resp._on_data_received()
    await asyncio.sleep(0)

    assert resp._heartbeat_cb is None
    assert resp._pong_response_cb is None
    assert resp._heartbeat_reset_handle is None
    assert resp._ping_task is None


async def test_data_received_after_close_does_not_resurrect_heartbeat() -> None:
    """Late frames after close must not reschedule the heartbeat."""
    response = mock.Mock()
    response.connection = None
    reader = mock.Mock()
    reader.read = mock.AsyncMock(
        return_value=WSMessageClose(data=1000, size=0, extra="close")
    )
    writer = mock.Mock()
    writer.close = mock.AsyncMock()
    resp = _make_client_ws(
        reader=reader,
        writer=writer,
        response=response,
        heartbeat=0.05,
        timeout=ClientWSTimeout(ws_receive=10.0, ws_close=10.0),
    )

    await resp.close()
    assert resp.closed

    resp._on_data_received()
    await asyncio.sleep(0)

    _assert_heartbeat_reclaimed(resp)


async def test_compressed_frame_then_close_single_close_frame() -> None:
    """Compressed frames followed by close: exactly one close frame."""
    response = mock.Mock()
    response.connection = None
    reader = mock.Mock()
    reader.read = mock.AsyncMock(
        return_value=WSMessageClose(data=1000, size=0, extra="close")
    )
    transport = mock.Mock()
    transport.is_closing.return_value = False
    writer = RealWebSocketWriter(
        mock.Mock(),
        transport,
        use_mask=True,
        random=random.Random(123),
        compress=15,
    )
    resp = _make_client_ws(
        reader=reader,
        writer=writer,
        response=response,
        heartbeat=0.05,
        timeout=ClientWSTimeout(ws_receive=10.0, ws_close=10.0),
    )

    await resp.send_str("compressed payload")
    await resp.close()

    written = [call[0][0] for call in transport.write.call_args_list]
    # The compressed text frame (FIN|RSV1|TEXT) went out before the close.
    assert written[0][:1] == b"\xc1"
    assert len(_close_frames_written(transport)) == 1
    assert resp.closed
    _assert_heartbeat_reclaimed(resp)


async def test_heartbeat_single_ping_timer_fake_loop() -> None:
    """One ping timer and one pong timeout per connection, fake loop."""
    fake_loop = mock.MagicMock()
    fake_loop.time.return_value = 100.0
    resp = _make_client_ws(heartbeat=10.0, loop=fake_loop)

    assert fake_loop.call_at.call_count == 1
    ping_timer = resp._heartbeat_cb
    assert ping_timer is not None

    # Receiving data coalesces into a single reset and never creates
    # a second ping timer or a pong timeout.
    resp._on_data_received()
    resp._on_data_received()
    assert fake_loop.call_soon.call_count == 1
    resp._flush_heartbeat_reset()
    assert fake_loop.call_at.call_count == 1
    assert resp._heartbeat_cb is ping_timer
    assert resp._pong_response_cb is None

    # A pending pong timeout is cancelled when new data arrives.
    pong_timeout = mock.MagicMock()
    resp._pong_response_cb = pong_timeout
    resp._reset_heartbeat()
    pong_timeout.cancel.assert_called_once()
    assert resp._pong_response_cb is None

    # Shutdown reclaims the ping timer.
    resp._cancel_heartbeat()
    ping_timer.cancel.assert_called_once()
    _assert_heartbeat_reclaimed(resp)


async def test_iterator_early_exit_then_clean_close() -> None:
    """Breaking out of async iteration must not break the close handshake."""
    response = mock.Mock()
    response.connection = None
    reader = mock.Mock()
    reader.read = mock.AsyncMock(
        side_effect=[
            WSMessageText(data="hello", size=5),
            WSMessageClose(data=1000, size=0, extra="close"),
        ]
    )
    writer = mock.Mock()
    writer.close = mock.AsyncMock()
    resp = _make_client_ws(
        reader=reader,
        writer=writer,
        response=response,
        heartbeat=0.05,
        timeout=ClientWSTimeout(ws_receive=10.0, ws_close=10.0),
    )

    async for msg in resp:
        assert msg.type == aiohttp.WSMsgType.TEXT
        break  # application stops iterating early

    await resp.close()
    assert resp.closed
    assert resp._close_code == 1000
    writer.close.assert_awaited_once()
    _assert_heartbeat_reclaimed(resp)

    # The closed connection cannot be resurrected.
    assert (await resp.receive()) is WS_CLOSED_MESSAGE


async def test_ws_connect_deflate_server_ext_bad(ws_key: str, key_data: bytes) -> None:
    resp = mock.Mock()
    resp.status = 101
    resp.headers = {
        hdrs.UPGRADE: "websocket",
        hdrs.CONNECTION: "upgrade",
        hdrs.SEC_WEBSOCKET_ACCEPT: ws_key,
        hdrs.SEC_WEBSOCKET_EXTENSIONS: "permessage-deflate; bad",
    }
    with mock.patch("aiohttp.client.os") as m_os:
        with mock.patch("aiohttp.client.ClientSession.request") as m_req:
            m_os.urandom.return_value = key_data
            m_req.return_value = asyncio.get_running_loop().create_future()
            m_req.return_value.set_result(resp)

            with pytest.raises(client.WSServerHandshakeError):
                await aiohttp.ClientSession().ws_connect("http://test.org", compress=15)
