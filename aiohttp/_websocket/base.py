"""Shared heartbeat and close-state machinery for WebSocket connections."""

from __future__ import annotations

import asyncio
import sys
from asyncio.base_events import BaseEventLoop
from typing import TYPE_CHECKING, Any

from ..helpers import calculate_timeout_when
from .models import WSCloseCode, WSMsgType, WSMessageError

if TYPE_CHECKING:
    from .reader import WebSocketDataQueue
    from .writer import WebSocketWriter


class WebSocketBase:
    """Shared heartbeat, ping/pong and terminal-state handling.

    Both the server (WebSocketResponse) and the client
    (ClientWebSocketResponse) drive their heartbeat and connection
    teardown through this single implementation so the behavior on the
    wire is identical on both sides:

    * At most one ping timer and one pong timeout are ever scheduled.
    * Any received data, pong, close, exception or shutdown updates the
      heartbeat state exactly once.
    * _set_closed() is the single terminal state: once reached, all
      heartbeat timers and the in-flight ping task are cancelled and can
      never be rescheduled, so a closed connection cannot be revived.
    """

    _loop: asyncio.AbstractEventLoop | None = None
    _writer: WebSocketWriter | None = None
    _reader: WebSocketDataQueue | None = None
    _parser: Any = None
    _closed: bool = False
    _closing: bool = False
    _close_code: int | None = None
    _waiting: bool = False
    _exception: BaseException | None = None
    _close_wait: asyncio.Future[None] | None = None
    _heartbeat: float | None = None
    _pong_heartbeat: float = 0.0
    _heartbeat_when: float = 0.0
    _heartbeat_cb: asyncio.TimerHandle | None = None
    _pong_response_cb: asyncio.TimerHandle | None = None
    _ping_task: asyncio.Task[None] | None = None
    _need_heartbeat_reset: bool = False
    _heartbeat_reset_handle: asyncio.Handle | None = None

    def _init_heartbeat(self, heartbeat: float | None) -> None:
        """Initialize the heartbeat state for the connection."""
        self._heartbeat = heartbeat
        if heartbeat is not None:
            self._pong_heartbeat = heartbeat / 2.0
        self._heartbeat_when = 0.0
        self._heartbeat_cb = None
        self._pong_response_cb = None
        self._ping_task = None
        self._need_heartbeat_reset = False
        self._heartbeat_reset_handle = None

    @property
    def _timeout_ceil_threshold(self) -> float:
        """Ceiling threshold used to avoid excessive timer rescheduling."""
        raise NotImplementedError

    @property
    def _transport_available(self) -> bool:
        """Return True if the underlying transport is still usable."""
        raise NotImplementedError

    def _pong_timeout_error(self) -> BaseException:
        """Return the exception to use when a pong is not received in time."""
        raise NotImplementedError

    def _abort_transport(self) -> None:
        """Forcibly close the underlying transport/connection."""
        raise NotImplementedError

    def _cancel_heartbeat(self) -> None:
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
        if self._heartbeat is None or self._need_heartbeat_reset:
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
        now = loop.time()
        when = calculate_timeout_when(
            now, self._heartbeat, self._timeout_ceil_threshold
        )
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

        when = calculate_timeout_when(
            now, self._pong_heartbeat, self._timeout_ceil_threshold
        )
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

    def _ping_task_done(self, task: asyncio.Task[None]) -> None:
        """Callback for when the ping task completes."""
        if not task.cancelled() and (exc := task.exception()):
            self._handle_ping_pong_exception(exc)
        # Only clear the reference if it still points at this task; a
        # heartbeat reset may have already replaced it with a newer ping
        # task which must remain tracked so it can be cancelled later.
        if self._ping_task is task:
            self._ping_task = None

    def _pong_not_received(self) -> None:
        if self._transport_available:
            self._handle_ping_pong_exception(self._pong_timeout_error())

    def _handle_ping_pong_exception(self, exc: BaseException) -> None:
        """Handle exceptions raised during ping/pong processing."""
        if self._closed:
            return
        self._set_closed()
        # close() is never reached after this; release the parser here.
        self._parser = None
        self._close_code = WSCloseCode.ABNORMAL_CLOSURE
        self._exception = exc
        self._abort_transport()
        reader = self._reader
        if self._waiting and not self._closing and reader is not None:
            reader.feed_data(WSMessageError(data=exc, extra=None))

    def _set_closed(self) -> None:
        """Set the connection to closed.

        Cancel any heartbeat timers and set the closed flag. This is the
        single terminal state of the connection; once set, no heartbeat
        timer or ping task may be scheduled again.
        """
        self._closed = True
        self._cancel_heartbeat()

    def _set_closing(self, code: int | None = None) -> None:
        """Set the connection to closing.

        Cancel any heartbeat timers and set the closing flag. If a close
        code is known it is recorded at the same time.
        """
        self._closing = True
        if code is not None:
            self._close_code = code
        self._cancel_heartbeat()
