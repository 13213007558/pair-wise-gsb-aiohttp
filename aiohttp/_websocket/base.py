"""Shared heartbeat and close-state machinery for WebSocket endpoints.

Both the client (:class:`aiohttp.client_ws.ClientWebSocketResponse`) and the
server (:class:`aiohttp.web_ws.WebSocketResponse`) implement the same
heartbeat, close handshake and cancellation semantics.  This module holds
the shared implementation so the two endpoints cannot drift apart.
"""

import asyncio
import sys
from asyncio.base_events import BaseEventLoop
from typing import TYPE_CHECKING

from ..helpers import calculate_timeout_when
from .models import WSCloseCode, WSMessageError, WSMsgType

if TYPE_CHECKING:
    from .reader import WebSocketDataQueue, WebSocketReader
    from .writer import WebSocketWriter


class WebSocketHeartbeatMixin:
    """Heartbeat (ping/pong keepalive) machinery for WebSocket endpoints.

    A connection has at most one ping timer (``_heartbeat_cb``) and one
    pong timeout (``_pong_response_cb``) at any moment:

    * Receiving any data reschedules the ping timer (coalesced so a burst of
      frames in a single loop tick only resets it once).
    * A pong -- like any other frame -- cancels the pending pong timeout.
    * Close, error, EOF and shutdown cancel both, so the heartbeat can never
      resurrect a closed connection.
    """

    if TYPE_CHECKING:
        _closed: bool
        _closing: bool
        _waiting: bool
        _close_code: int | None
        _exception: BaseException | None
        _heartbeat: float | None
        _pong_heartbeat: float
        _heartbeat_cb: asyncio.TimerHandle | None
        _heartbeat_when: float
        _pong_response_cb: asyncio.TimerHandle | None
        _ping_task: asyncio.Task[None] | None
        _need_heartbeat_reset: bool
        _heartbeat_reset_handle: asyncio.Handle | None
        _loop: asyncio.AbstractEventLoop | None
        _writer: WebSocketWriter | None
        _reader: WebSocketDataQueue | None
        _parser: WebSocketReader | None

    def _init_heartbeat(self, heartbeat: float | None) -> None:
        """Initialize the heartbeat state; must be called once from __init__."""
        self._heartbeat = heartbeat
        self._heartbeat_cb = None
        self._heartbeat_when = 0.0
        if heartbeat is not None:
            self._pong_heartbeat = heartbeat / 2.0
        self._pong_response_cb = None
        self._ping_task = None
        self._need_heartbeat_reset = False
        self._heartbeat_reset_handle = None

    def _get_timeout_ceil_threshold(self) -> float:
        """Return the timeout ceil threshold of the owning connection."""
        raise NotImplementedError

    def _make_pong_timeout_error(self) -> BaseException:
        """Return the exception to raise when a pong is not received in time."""
        raise NotImplementedError

    def _close_transport_on_error(self) -> None:
        """Close the underlying transport after a heartbeat failure."""
        raise NotImplementedError

    def _cancel_heartbeat(self) -> None:
        """Cancel every heartbeat timer and task.

        After this call the connection has no pending ping timer, pong
        timeout, ping task or coalesced reset handle, so the heartbeat
        machinery can be fully reclaimed by the event loop.
        """
        self._cancel_pong_response_cb()
        if self._heartbeat_reset_handle is not None:
            self._heartbeat_reset_handle.cancel()
            self._heartbeat_reset_handle = None
        self._need_heartbeat_reset = False
        if self._heartbeat_cb is not None:
            self._heartbeat_cb.cancel()
            self._heartbeat_cb = None
        if self._ping_task is not None:
            self._ping_task.cancel()
            self._ping_task = None

    def _cancel_pong_response_cb(self) -> None:
        if self._pong_response_cb is not None:
            self._pong_response_cb.cancel()
            self._pong_response_cb = None

    def _on_data_received(self) -> None:
        if self._heartbeat is None or self._need_heartbeat_reset or self._closed:
            return
        loop = self._loop
        assert loop is not None
        # Coalesce multiple chunks received in the same loop tick into a single
        # heartbeat reset. Resetting immediately per chunk increases timer churn.
        self._need_heartbeat_reset = True
        self._heartbeat_reset_handle = loop.call_soon(self._flush_heartbeat_reset)

    def _flush_heartbeat_reset(self) -> None:
        self._heartbeat_reset_handle = None
        if not self._need_heartbeat_reset:
            return
        self._reset_heartbeat()
        self._need_heartbeat_reset = False

    def _reset_heartbeat(self) -> None:
        if self._heartbeat is None:
            return
        self._cancel_pong_response_cb()
        loop = self._loop
        assert loop is not None
        timeout_ceil_threshold = self._get_timeout_ceil_threshold()
        now = loop.time()
        when = calculate_timeout_when(now, self._heartbeat, timeout_ceil_threshold)
        self._heartbeat_when = when
        if self._heartbeat_cb is None:
            # We do not cancel the previous heartbeat_cb here because
            # it generates a significant amount of TimerHandle churn
            # which causes asyncio to rebuild the heap frequently.
            # Instead _send_heartbeat() will reschedule the next
            # heartbeat if it fires too early.
            self._heartbeat_cb = loop.call_at(when, self._send_heartbeat)

    def _send_heartbeat(self) -> None:
        self._heartbeat_cb = None

        # If heartbeat reset is pending (data is being received), skip sending
        # the ping and let the reset callback handle rescheduling the heartbeat.
        if self._need_heartbeat_reset:
            return

        loop = self._loop
        assert loop is not None and self._writer is not None
        now = loop.time()
        if now < self._heartbeat_when:
            # Heartbeat fired too early, reschedule
            self._heartbeat_cb = loop.call_at(
                self._heartbeat_when, self._send_heartbeat
            )
            return

        timeout_ceil_threshold = self._get_timeout_ceil_threshold()
        when = calculate_timeout_when(now, self._pong_heartbeat, timeout_ceil_threshold)
        self._cancel_pong_response_cb()
        self._pong_response_cb = loop.call_at(when, self._pong_not_received)

        coro = self._writer.send_frame(b"", WSMsgType.PING)
        if sys.version_info >= (3, 14):
            # Try to send the ping immediately to avoid having to schedule
            # the task on the event loop.
            if isinstance(loop, BaseEventLoop):
                ping_task = asyncio.create_task(coro, eager_start=True)
            else:
                ping_task = asyncio.Task(coro, loop=loop, eager_start=True)
        elif sys.version_info >= (3, 12):
            ping_task = asyncio.Task(coro, loop=loop, eager_start=True)
        else:
            ping_task = asyncio.create_task(coro)

        if not ping_task.done():
            self._ping_task = ping_task
            ping_task.add_done_callback(self._ping_task_done)
        else:
            self._ping_task_done(ping_task)

    def _ping_task_done(self, task: "asyncio.Task[None]") -> None:
        """Callback for when the ping task completes."""
        if not task.cancelled() and (exc := task.exception()):
            self._handle_ping_pong_exception(exc)
        self._ping_task = None

    def _pong_not_received(self) -> None:
        # The pong timeout always drives the connection to the closed
        # terminal state, even if the transport is already gone, so the
        # connection can never be stuck waiting for a pong forever.
        self._handle_ping_pong_exception(self._make_pong_timeout_error())

    def _handle_ping_pong_exception(self, exc: BaseException) -> None:
        """Handle exceptions raised during ping/pong processing."""
        if self._closed:
            return
        self._set_closed()
        # close() is never reached after this; release the parser here.
        self._parser = None
        self._close_code = WSCloseCode.ABNORMAL_CLOSURE
        self._exception = exc
        self._close_transport_on_error()
        reader = self._reader
        if self._waiting and not self._closing and reader is not None:
            reader.feed_data(WSMessageError(data=exc, extra=None))

    def _set_closed(self) -> None:
        """Set the connection to closed.

        Cancel any heartbeat timers and set the closed flag.
        """
        self._closed = True
        self._cancel_heartbeat()

    def _set_closing(self, code: int | None = None) -> None:
        """Set the connection to closing.

        Cancel any heartbeat timers and set the closing flag.
        """
        self._closing = True
        if code is not None:
            self._close_code = code
        self._cancel_heartbeat()
