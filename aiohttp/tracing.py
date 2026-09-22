from types import SimpleNamespace
from typing import TYPE_CHECKING, Any, Generic, Protocol, TypeVar, overload

from aiosignal import Signal
from multidict import CIMultiDict
from yarl import URL

from .client_reqrep import ClientResponse
from .helpers import frozen_dataclass_decorator
from .log import client_logger

if TYPE_CHECKING:
    from .client import ClientSession


__all__ = (
    "TraceConfig",
    "TraceRequestStartParams",
    "TraceRequestEndParams",
    "TraceRequestExceptionParams",
    "TraceConnectionQueuedStartParams",
    "TraceConnectionQueuedEndParams",
    "TraceConnectionCreateStartParams",
    "TraceConnectionCreateEndParams",
    "TraceConnectionReuseconnParams",
    "TraceDnsResolveHostStartParams",
    "TraceDnsResolveHostEndParams",
    "TraceDnsCacheHitParams",
    "TraceDnsCacheMissParams",
    "TraceRequestRedirectParams",
    "TraceRequestChunkSentParams",
    "TraceResponseChunkReceivedParams",
    "TraceRequestHeadersSentParams",
)

_T = TypeVar("_T", covariant=True)
_ParamT_contra = TypeVar("_ParamT_contra", contravariant=True)
_TracingSignal = Signal["ClientSession", _T, _ParamT_contra]


class _Factory(Protocol[_T]):
    def __call__(self, **kwargs: Any) -> _T: ...


class TraceConfig(Generic[_T]):
    """First-class used to trace requests launched via ClientSession objects."""

    @overload
    def __init__(self: "TraceConfig[SimpleNamespace]") -> None: ...
    @overload
    def __init__(self, trace_config_ctx_factory: _Factory[_T]) -> None: ...
    def __init__(
        self, trace_config_ctx_factory: _Factory[Any] = SimpleNamespace
    ) -> None:
        self._on_request_start: _TracingSignal[_T, TraceRequestStartParams] = Signal(
            self
        )
        self._on_request_chunk_sent: _TracingSignal[_T, TraceRequestChunkSentParams] = (
            Signal(self)
        )
        self._on_response_chunk_received: _TracingSignal[
            _T, TraceResponseChunkReceivedParams
        ] = Signal(self)
        self._on_request_end: _TracingSignal[_T, TraceRequestEndParams] = Signal(self)
        self._on_request_exception: _TracingSignal[_T, TraceRequestExceptionParams] = (
            Signal(self)
        )
        self._on_request_redirect: _TracingSignal[_T, TraceRequestRedirectParams] = (
            Signal(self)
        )
        self._on_connection_queued_start: _TracingSignal[
            _T, TraceConnectionQueuedStartParams
        ] = Signal(self)
        self._on_connection_queued_end: _TracingSignal[
            _T, TraceConnectionQueuedEndParams
        ] = Signal(self)
        self._on_connection_create_start: _TracingSignal[
            _T, TraceConnectionCreateStartParams
        ] = Signal(self)
        self._on_connection_create_end: _TracingSignal[
            _T, TraceConnectionCreateEndParams
        ] = Signal(self)
        self._on_connection_reuseconn: _TracingSignal[
            _T, TraceConnectionReuseconnParams
        ] = Signal(self)
        self._on_dns_resolvehost_start: _TracingSignal[
            _T, TraceDnsResolveHostStartParams
        ] = Signal(self)
        self._on_dns_resolvehost_end: _TracingSignal[
            _T, TraceDnsResolveHostEndParams
        ] = Signal(self)
        self._on_dns_cache_hit: _TracingSignal[_T, TraceDnsCacheHitParams] = Signal(
            self
        )
        self._on_dns_cache_miss: _TracingSignal[_T, TraceDnsCacheMissParams] = Signal(
            self
        )
        self._on_request_headers_sent: _TracingSignal[
            _T, TraceRequestHeadersSentParams
        ] = Signal(self)

        self._trace_config_ctx_factory: _Factory[_T] = trace_config_ctx_factory

    def trace_config_ctx(self, trace_request_ctx: Any = None) -> _T:
        """Return a new trace_config_ctx instance"""
        return self._trace_config_ctx_factory(trace_request_ctx=trace_request_ctx)

    def freeze(self) -> None:
        self._on_request_start.freeze()
        self._on_request_chunk_sent.freeze()
        self._on_response_chunk_received.freeze()
        self._on_request_end.freeze()
        self._on_request_exception.freeze()
        self._on_request_redirect.freeze()
        self._on_connection_queued_start.freeze()
        self._on_connection_queued_end.freeze()
        self._on_connection_create_start.freeze()
        self._on_connection_create_end.freeze()
        self._on_connection_reuseconn.freeze()
        self._on_dns_resolvehost_start.freeze()
        self._on_dns_resolvehost_end.freeze()
        self._on_dns_cache_hit.freeze()
        self._on_dns_cache_miss.freeze()
        self._on_request_headers_sent.freeze()

    @property
    def on_request_start(self) -> "_TracingSignal[_T, TraceRequestStartParams]":
        return self._on_request_start

    @property
    def on_request_chunk_sent(
        self,
    ) -> "_TracingSignal[_T, TraceRequestChunkSentParams]":
        return self._on_request_chunk_sent

    @property
    def on_response_chunk_received(
        self,
    ) -> "_TracingSignal[_T, TraceResponseChunkReceivedParams]":
        return self._on_response_chunk_received

    @property
    def on_request_end(self) -> "_TracingSignal[_T, TraceRequestEndParams]":
        return self._on_request_end

    @property
    def on_request_exception(
        self,
    ) -> "_TracingSignal[_T, TraceRequestExceptionParams]":
        return self._on_request_exception

    @property
    def on_request_redirect(
        self,
    ) -> "_TracingSignal[_T, TraceRequestRedirectParams]":
        return self._on_request_redirect

    @property
    def on_connection_queued_start(
        self,
    ) -> "_TracingSignal[_T, TraceConnectionQueuedStartParams]":
        return self._on_connection_queued_start

    @property
    def on_connection_queued_end(
        self,
    ) -> "_TracingSignal[_T, TraceConnectionQueuedEndParams]":
        return self._on_connection_queued_end

    @property
    def on_connection_create_start(
        self,
    ) -> "_TracingSignal[_T, TraceConnectionCreateStartParams]":
        return self._on_connection_create_start

    @property
    def on_connection_create_end(
        self,
    ) -> "_TracingSignal[_T, TraceConnectionCreateEndParams]":
        return self._on_connection_create_end

    @property
    def on_connection_reuseconn(
        self,
    ) -> "_TracingSignal[_T, TraceConnectionReuseconnParams]":
        return self._on_connection_reuseconn

    @property
    def on_dns_resolvehost_start(
        self,
    ) -> "_TracingSignal[_T, TraceDnsResolveHostStartParams]":
        return self._on_dns_resolvehost_start

    @property
    def on_dns_resolvehost_end(
        self,
    ) -> "_TracingSignal[_T, TraceDnsResolveHostEndParams]":
        return self._on_dns_resolvehost_end

    @property
    def on_dns_cache_hit(self) -> "_TracingSignal[_T, TraceDnsCacheHitParams]":
        return self._on_dns_cache_hit

    @property
    def on_dns_cache_miss(self) -> "_TracingSignal[_T, TraceDnsCacheMissParams]":
        return self._on_dns_cache_miss

    @property
    def on_request_headers_sent(
        self,
    ) -> "_TracingSignal[_T, TraceRequestHeadersSentParams]":
        return self._on_request_headers_sent


@frozen_dataclass_decorator
class TraceRequestStartParams:
    """Parameters sent by the `on_request_start` signal"""

    method: str
    url: URL
    headers: "CIMultiDict[str]"


@frozen_dataclass_decorator
class TraceRequestChunkSentParams:
    """Parameters sent by the `on_request_chunk_sent` signal"""

    method: str
    url: URL
    chunk: bytes


@frozen_dataclass_decorator
class TraceResponseChunkReceivedParams:
    """Parameters sent by the `on_response_chunk_received` signal"""

    method: str
    url: URL
    chunk: bytes


@frozen_dataclass_decorator
class TraceRequestEndParams:
    """Parameters sent by the `on_request_end` signal"""

    method: str
    url: URL
    headers: "CIMultiDict[str]"
    response: ClientResponse


@frozen_dataclass_decorator
class TraceRequestExceptionParams:
    """Parameters sent by the `on_request_exception` signal"""

    method: str
    url: URL
    headers: "CIMultiDict[str]"
    exception: BaseException


@frozen_dataclass_decorator
class TraceRequestRedirectParams:
    """Parameters sent by the `on_request_redirect` signal"""

    method: str
    url: URL
    headers: "CIMultiDict[str]"
    response: ClientResponse


@frozen_dataclass_decorator
class TraceConnectionQueuedStartParams:
    """Parameters sent by the `on_connection_queued_start` signal"""


@frozen_dataclass_decorator
class TraceConnectionQueuedEndParams:
    """Parameters sent by the `on_connection_queued_end` signal"""


@frozen_dataclass_decorator
class TraceConnectionCreateStartParams:
    """Parameters sent by the `on_connection_create_start` signal"""


@frozen_dataclass_decorator
class TraceConnectionCreateEndParams:
    """Parameters sent by the `on_connection_create_end` signal"""


@frozen_dataclass_decorator
class TraceConnectionReuseconnParams:
    """Parameters sent by the `on_connection_reuseconn` signal"""


@frozen_dataclass_decorator
class TraceDnsResolveHostStartParams:
    """Parameters sent by the `on_dns_resolvehost_start` signal"""

    host: str


@frozen_dataclass_decorator
class TraceDnsResolveHostEndParams:
    """Parameters sent by the `on_dns_resolvehost_end` signal"""

    host: str


@frozen_dataclass_decorator
class TraceDnsCacheHitParams:
    """Parameters sent by the `on_dns_cache_hit` signal"""

    host: str


@frozen_dataclass_decorator
class TraceDnsCacheMissParams:
    """Parameters sent by the `on_dns_cache_miss` signal"""

    host: str


@frozen_dataclass_decorator
class TraceRequestHeadersSentParams:
    """Parameters sent by the `on_request_headers_sent` signal"""

    method: str
    url: URL
    headers: "CIMultiDict[str]"


class Trace:
    """Internal dependency holder class.

    Used to keep together the main dependencies used
    at the moment of send a signal.

    Exceptions raised by trace callbacks are logged and swallowed for
    every signal except `on_response_chunk_received`, so a misbehaving
    callback can never change the outcome of the request being traced.
    `on_response_chunk_received` is delivered from the response stream
    while the caller is reading it; there an error is re-raised to the
    reader and the response is closed instead.
    """

    def __init__(
        self,
        session: "ClientSession",
        trace_config: TraceConfig[object],
        trace_config_ctx: Any,
    ) -> None:
        self._trace_config = trace_config
        self._trace_config_ctx = trace_config_ctx
        self._session = session

    async def _dispatch(
        self, name: str, signal: "_TracingSignal[Any, Any]", params: object
    ) -> None:
        try:
            await signal.send(self._session, self._trace_config_ctx, params)
        except Exception:
            client_logger.exception("Error in %s trace callback", name)

    async def send_request_start(
        self, method: str, url: URL, headers: "CIMultiDict[str]"
    ) -> None:
        await self._dispatch(
            "on_request_start",
            self._trace_config.on_request_start,
            TraceRequestStartParams(method, url, headers),
        )

    async def send_request_chunk_sent(
        self, method: str, url: URL, chunk: bytes
    ) -> None:
        await self._dispatch(
            "on_request_chunk_sent",
            self._trace_config.on_request_chunk_sent,
            TraceRequestChunkSentParams(method, url, chunk),
        )

    async def send_response_chunk_received(
        self, method: str, url: URL, chunk: bytes
    ) -> None:
        return await self._trace_config.on_response_chunk_received.send(
            self._session,
            self._trace_config_ctx,
            TraceResponseChunkReceivedParams(method, url, chunk),
        )

    async def send_request_end(
        self,
        method: str,
        url: URL,
        headers: "CIMultiDict[str]",
        response: ClientResponse,
    ) -> None:
        await self._dispatch(
            "on_request_end",
            self._trace_config.on_request_end,
            TraceRequestEndParams(method, url, headers, response),
        )

    async def send_request_exception(
        self,
        method: str,
        url: URL,
        headers: "CIMultiDict[str]",
        exception: BaseException,
    ) -> None:
        await self._dispatch(
            "on_request_exception",
            self._trace_config.on_request_exception,
            TraceRequestExceptionParams(method, url, headers, exception),
        )

    async def send_request_redirect(
        self,
        method: str,
        url: URL,
        headers: "CIMultiDict[str]",
        response: ClientResponse,
    ) -> None:
        await self._dispatch(
            "on_request_redirect",
            self._trace_config.on_request_redirect,
            TraceRequestRedirectParams(method, url, headers, response),
        )

    async def send_connection_queued_start(self) -> None:
        await self._dispatch(
            "on_connection_queued_start",
            self._trace_config.on_connection_queued_start,
            TraceConnectionQueuedStartParams(),
        )

    async def send_connection_queued_end(self) -> None:
        await self._dispatch(
            "on_connection_queued_end",
            self._trace_config.on_connection_queued_end,
            TraceConnectionQueuedEndParams(),
        )

    async def send_connection_create_start(self) -> None:
        await self._dispatch(
            "on_connection_create_start",
            self._trace_config.on_connection_create_start,
            TraceConnectionCreateStartParams(),
        )

    async def send_connection_create_end(self) -> None:
        await self._dispatch(
            "on_connection_create_end",
            self._trace_config.on_connection_create_end,
            TraceConnectionCreateEndParams(),
        )

    async def send_connection_reuseconn(self) -> None:
        await self._dispatch(
            "on_connection_reuseconn",
            self._trace_config.on_connection_reuseconn,
            TraceConnectionReuseconnParams(),
        )

    async def send_dns_resolvehost_start(self, host: str) -> None:
        await self._dispatch(
            "on_dns_resolvehost_start",
            self._trace_config.on_dns_resolvehost_start,
            TraceDnsResolveHostStartParams(host),
        )

    async def send_dns_resolvehost_end(self, host: str) -> None:
        await self._dispatch(
            "on_dns_resolvehost_end",
            self._trace_config.on_dns_resolvehost_end,
            TraceDnsResolveHostEndParams(host),
        )

    async def send_dns_cache_hit(self, host: str) -> None:
        await self._dispatch(
            "on_dns_cache_hit",
            self._trace_config.on_dns_cache_hit,
            TraceDnsCacheHitParams(host),
        )

    async def send_dns_cache_miss(self, host: str) -> None:
        await self._dispatch(
            "on_dns_cache_miss",
            self._trace_config.on_dns_cache_miss,
            TraceDnsCacheMissParams(host),
        )

    async def send_request_headers(
        self, method: str, url: URL, headers: "CIMultiDict[str]"
    ) -> None:
        await self._dispatch(
            "on_request_headers_sent",
            self._trace_config.on_request_headers_sent,
            TraceRequestHeadersSentParams(method, url, headers),
        )
