"""WebSocket client for asyncio."""

import asyncio
import sys
from collections.abc import Callable
from types import TracebackType
from typing import Any, Final, Generic, Literal, overload

from ._websocket.base import WebSocketHeartbeatMixin
from ._websocket.reader import WebSocketDataQueue, WebSocketReader
from .client_exceptions import ClientError, ServerTimeoutError, WSMessageTypeError
from .client_reqrep import ClientResponse
from .helpers import frozen_dataclass_decorator, set_result
from .http import (
    WS_CLOSED_MESSAGE,
    WS_CLOSING_MESSAGE,
    WebSocketError,
    WSCloseCode,
    WSMessageDecodeText,
    WSMessageNoDecodeText,
    WSMsgType,
)
from .http_websocket import _INTERNAL_RECEIVE_TYPES, WebSocketWriter, WSMessageError
from .streams import EofStream
from .typedefs import (
    DEFAULT_JSON_DECODER,
    DEFAULT_JSON_ENCODER,
    JSONBytesEncoder,
    JSONDecoder,
    JSONEncoder,
)

if sys.version_info >= (3, 13):
    from typing import TypeVar
else:
    from typing_extensions import TypeVar

if sys.version_info >= (3, 11):
    import asyncio as async_timeout
    from typing import Self
else:
    import async_timeout
    from typing_extensions import Self

# TypeVar for whether text messages are decoded to str (True) or kept as bytes (False)
# Covariant because it only affects return types, not input types
_DecodeText = TypeVar("_DecodeText", bound=bool, covariant=True, default=Literal[True])


@frozen_dataclass_decorator
class ClientWSTimeout:
    ws_receive: float | None = None
    ws_close: float | None = None


DEFAULT_WS_CLIENT_TIMEOUT: Final[ClientWSTimeout] = ClientWSTimeout(
    ws_receive=None, ws_close=10.0
)


class ClientWebSocketResponse(WebSocketHeartbeatMixin, Generic[_DecodeText]):
    # Narrow the shared heartbeat state: on the client the writer, reader
    # and loop are always available right after construction.
    _writer: WebSocketWriter
    _reader: WebSocketDataQueue
    _loop: asyncio.AbstractEventLoop

    def __init__(
        self,
        reader: WebSocketDataQueue,
        writer: WebSocketWriter,
        protocol: str | None,
        response: ClientResponse,
        timeout: ClientWSTimeout,
        autoclose: bool,
        autoping: bool,
        loop: asyncio.AbstractEventLoop,
        *,
        heartbeat: float | None = None,
        compress: int = 0,
        client_notakeover: bool = False,
    ) -> None:
        self._response = response
        self._conn = response.connection

        self._writer = writer
        self._reader = reader
        # Set by ClientSession._ws_connect; owns the parser so a stalled
        # reader parked on the queue by weakref stays alive while this
        # response can still be drained.
        self._parser: WebSocketReader | None = None
        self._protocol = protocol
        self._closed = False
        self._closing = False
        self._close_code: int | None = None
        self._timeout = timeout
        self._autoclose = autoclose
        self._autoping = autoping
        self._init_heartbeat(heartbeat)
        self._loop = loop
        self._waiting: bool = False
        self._close_wait: asyncio.Future[None] | None = None
        self._exception: BaseException | None = None
        self._compress = compress
        self._client_notakeover = client_notakeover

        self._reset_heartbeat()

    def _get_timeout_ceil_threshold(self) -> float:
        conn = self._conn
        return conn._connector._timeout_ceil_threshold if conn is not None else 5

    def _make_pong_timeout_error(self) -> BaseException:
        return ServerTimeoutError(
            f"No PONG received after {self._pong_heartbeat} seconds"
        )

    def _close_transport_on_error(self) -> None:
        self._response.close()

    @property
    def closed(self) -> bool:
        return self._closed

    @property
    def close_code(self) -> int | None:
        return self._close_code

    @property
    def protocol(self) -> str | None:
        return self._protocol

    @property
    def compress(self) -> int:
        return self._compress

    @property
    def client_notakeover(self) -> bool:
        return self._client_notakeover

    def get_extra_info(self, name: str, default: Any = None) -> Any:
        """extra info from connection transport"""
        conn = self._response.connection
        if conn is None:
            return default
        transport = conn.transport
        if transport is None:
            return default
        return transport.get_extra_info(name, default)

    def exception(self) -> BaseException | None:
        return self._exception

    async def ping(self, message: bytes = b"") -> None:
        await self._writer.send_frame(message, WSMsgType.PING)

    async def pong(self, message: bytes = b"") -> None:
        await self._writer.send_frame(message, WSMsgType.PONG)

    async def send_frame(
        self, message: bytes, opcode: WSMsgType, compress: int | None = None
    ) -> None:
        """Send a frame over the websocket."""
        await self._writer.send_frame(message, opcode, compress)

    async def send_str(self, data: str, compress: int | None = None) -> None:
        if not isinstance(data, str):
            raise TypeError("data argument must be str (%r)" % type(data))
        await self._writer.send_frame(
            data.encode("utf-8"), WSMsgType.TEXT, compress=compress
        )

    async def send_bytes(self, data: bytes, compress: int | None = None) -> None:
        if not isinstance(data, (bytes, bytearray, memoryview)):
            raise TypeError("data argument must be byte-ish (%r)" % type(data))
        await self._writer.send_frame(data, WSMsgType.BINARY, compress=compress)

    async def send_json(
        self,
        data: Any,
        compress: int | None = None,
        *,
        dumps: JSONEncoder = DEFAULT_JSON_ENCODER,
    ) -> None:
        await self.send_str(dumps(data), compress=compress)

    async def send_json_bytes(
        self,
        data: Any,
        compress: int | None = None,
        *,
        dumps: JSONBytesEncoder,
    ) -> None:
        """Send JSON data using a bytes-returning encoder as a binary frame.

        Use this when your JSON encoder (like orjson) returns bytes
        instead of str, avoiding the encode/decode overhead.
        """
        await self.send_bytes(dumps(data), compress=compress)

    async def close(self, *, code: int = WSCloseCode.OK, message: bytes = b"") -> bool:
        # we need to break `receive()` cycle first,
        # `close()` may be called from different task
        if self._waiting and not self._closing:
            assert self._loop is not None
            self._close_wait = self._loop.create_future()
            self._set_closing()
            self._reader.feed_data(WS_CLOSING_MESSAGE)
            await self._close_wait

        if self._closed:
            return False

        self._set_closed()
        try:
            try:
                await self._writer.close(code, message)
            except asyncio.CancelledError:
                self._close_code = WSCloseCode.ABNORMAL_CLOSURE
                self._response.close()
                raise
            except Exception as exc:
                self._close_code = WSCloseCode.ABNORMAL_CLOSURE
                self._exception = exc
                self._response.close()
                return True

            if self._close_code:
                self._response.close()
                return True

            while True:
                try:
                    async with async_timeout.timeout(self._timeout.ws_close):
                        msg = await self._reader.read()
                except asyncio.CancelledError:
                    self._close_code = WSCloseCode.ABNORMAL_CLOSURE
                    self._response.close()
                    raise
                except Exception as exc:
                    self._close_code = WSCloseCode.ABNORMAL_CLOSURE
                    self._exception = exc
                    self._response.close()
                    return True

                if msg.type is WSMsgType.CLOSE:
                    self._close_code = msg.data
                    self._response.close()
                    return True
        finally:
            # Once closed the response can no longer be drained; release the
            # parser and the stash it retains.
            self._parser = None

    @overload
    async def receive(
        self: "ClientWebSocketResponse[Literal[True]]", timeout: float | None = None
    ) -> WSMessageDecodeText: ...

    @overload
    async def receive(
        self: "ClientWebSocketResponse[Literal[False]]", timeout: float | None = None
    ) -> WSMessageNoDecodeText: ...

    @overload
    async def receive(
        self: "ClientWebSocketResponse[_DecodeText]", timeout: float | None = None
    ) -> WSMessageDecodeText | WSMessageNoDecodeText: ...

    async def receive(
        self, timeout: float | None = None
    ) -> WSMessageDecodeText | WSMessageNoDecodeText:
        receive_timeout = timeout or self._timeout.ws_receive

        while True:
            if self._waiting:
                raise RuntimeError("Concurrent call to receive() is not allowed")

            if self._closed:
                return WS_CLOSED_MESSAGE
            elif self._closing:
                await self.close()
                return WS_CLOSED_MESSAGE

            try:
                self._waiting = True
                try:
                    if receive_timeout:
                        # Entering the context manager and creating
                        # Timeout() object can take almost 50% of the
                        # run time in this loop so we avoid it if
                        # there is no read timeout.
                        async with async_timeout.timeout(receive_timeout):
                            msg = await self._reader.read()
                    else:
                        msg = await self._reader.read()
                finally:
                    self._waiting = False
                    if self._close_wait:
                        set_result(self._close_wait, None)
            except asyncio.CancelledError:
                # The application cancelled the receive; make sure the
                # heartbeat timers and ping task are reclaimed instead of
                # keeping the connection alive after it was abandoned.
                self._cancel_heartbeat()
                self._close_code = WSCloseCode.ABNORMAL_CLOSURE
                raise
            except asyncio.TimeoutError:
                self._close_code = WSCloseCode.ABNORMAL_CLOSURE
                raise
            except EofStream:
                self._close_code = WSCloseCode.OK
                await self.close()
                return WS_CLOSED_MESSAGE
            except ClientError:
                # Likely ServerDisconnectedError when connection is lost.
                # close() is not called on this path, so release the parser
                # and the stash it retains here.
                self._parser = None
                self._set_closed()
                self._close_code = WSCloseCode.ABNORMAL_CLOSURE
                return WS_CLOSED_MESSAGE
            except WebSocketError as exc:
                self._close_code = exc.code
                await self.close(code=exc.code)
                return WSMessageError(data=exc)
            except Exception as exc:
                self._exception = exc
                self._set_closing(WSCloseCode.ABNORMAL_CLOSURE)
                await self.close()
                return WSMessageError(data=exc)

            if msg.type not in _INTERNAL_RECEIVE_TYPES:
                # If its not a close/closing/ping/pong message
                # we can return it immediately
                return msg

            if msg.type is WSMsgType.CLOSE:
                self._set_closing(msg.data)
                # Could be closed elsewhere while awaiting reader
                if not self._closed and self._autoclose:  # type: ignore[redundant-expr]
                    await self.close()
            elif msg.type is WSMsgType.CLOSING:
                self._set_closing()
            elif msg.type is WSMsgType.PING and self._autoping:
                await self.pong(msg.data)
                continue
            elif msg.type is WSMsgType.PONG and self._autoping:
                continue

            return msg

    @overload
    async def receive_str(
        self: "ClientWebSocketResponse[Literal[True]]", *, timeout: float | None = None
    ) -> str: ...

    @overload
    async def receive_str(
        self: "ClientWebSocketResponse[Literal[False]]", *, timeout: float | None = None
    ) -> bytes: ...

    @overload
    async def receive_str(
        self: "ClientWebSocketResponse[_DecodeText]", *, timeout: float | None = None
    ) -> str | bytes: ...

    async def receive_str(self, *, timeout: float | None = None) -> str | bytes:
        """Receive TEXT message.

        Returns str when decode_text=True (default), bytes when decode_text=False.
        """
        msg = await self.receive(timeout)
        if msg.type is not WSMsgType.TEXT:
            raise WSMessageTypeError(
                f"Received message {msg.type}:{msg.data!r} is not WSMsgType.TEXT"
            )
        return msg.data

    async def receive_bytes(self, *, timeout: float | None = None) -> bytes:
        msg = await self.receive(timeout)
        if msg.type is not WSMsgType.BINARY:
            raise WSMessageTypeError(
                f"Received message {msg.type}:{msg.data!r} is not WSMsgType.BINARY"
            )
        return msg.data

    @overload
    async def receive_json(
        self: "ClientWebSocketResponse[Literal[True]]",
        *,
        loads: JSONDecoder = ...,
        timeout: float | None = None,
    ) -> Any: ...

    @overload
    async def receive_json(
        self: "ClientWebSocketResponse[Literal[False]]",
        *,
        loads: Callable[[bytes], Any] = ...,
        timeout: float | None = None,
    ) -> Any: ...

    @overload
    async def receive_json(
        self: "ClientWebSocketResponse[_DecodeText]",
        *,
        loads: JSONDecoder | Callable[[bytes], Any] = ...,
        timeout: float | None = None,
    ) -> Any: ...

    async def receive_json(
        self,
        *,
        loads: JSONDecoder | Callable[[bytes], Any] = DEFAULT_JSON_DECODER,
        timeout: float | None = None,
    ) -> Any:
        data = await self.receive_str(timeout=timeout)
        return loads(data)  # type: ignore[arg-type]

    def __aiter__(self) -> Self:
        return self

    @overload
    async def __anext__(
        self: "ClientWebSocketResponse[Literal[True]]",
    ) -> WSMessageDecodeText: ...

    @overload
    async def __anext__(
        self: "ClientWebSocketResponse[Literal[False]]",
    ) -> WSMessageNoDecodeText: ...

    @overload
    async def __anext__(
        self: "ClientWebSocketResponse[_DecodeText]",
    ) -> WSMessageDecodeText | WSMessageNoDecodeText: ...

    async def __anext__(self) -> WSMessageDecodeText | WSMessageNoDecodeText:
        msg = await self.receive()
        if msg.type in (WSMsgType.CLOSE, WSMsgType.CLOSING, WSMsgType.CLOSED):
            raise StopAsyncIteration
        return msg

    async def __aenter__(self) -> Self:
        return self

    async def __aexit__(
        self,
        exc_type: type[BaseException] | None,
        exc_val: BaseException | None,
        exc_tb: TracebackType | None,
    ) -> None:
        await self.close()
