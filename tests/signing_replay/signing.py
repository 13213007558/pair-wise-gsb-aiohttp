"""One-shot request signing middleware and replay policy.

The signature of every request attempt binds three things:

* the request method,
* the request target that goes on the wire (``raw_path_qs``),
* a digest of the body bytes of that attempt.

The module deliberately keeps the secret out of every log structure: audits
record the public signature *sequence number* and body digests, never the
HMAC value or the key.
"""

import hashlib
import hmac
from collections.abc import AsyncIterator, Callable
from dataclasses import dataclass, field

from yarl import URL

from aiohttp import hdrs
from aiohttp.abc import AbstractStreamWriter
from aiohttp.client_exceptions import ClientError
from aiohttp.client_middlewares import ClientHandlerType
from aiohttp.client_reqrep import ClientRequest, ClientResponse
from aiohttp.payload import (
    AsyncIterablePayload,
    BytesPayload,
    IOBasePayload,
    Payload,
)
from aiohttp.tracing import TraceConfig, TraceRequestRedirectParams

SIGNATURE_HEADER = "X-Request-Signature"
BODY_DIGEST_HEADER = "X-Body-Sha256"
SEQUENCE_HEADER = "X-Signature-Seq"

# Fixed test-only key. It is never written to any audit record.
TEST_KEY = b"unit-test-signing-key"

_EMPTY_BODY_DIGEST = hashlib.sha256(b"").hexdigest()


class UnreplayableBodyError(ClientError):
    """Raised when a one-shot body would be needed for a second wire attempt."""


@dataclass
class OneShotAsyncIterable:
    """An async iterable that can only produce its chunks once.

    The digest of the chunks is computed when the object is built, so the
    first (and only) attempt can be signed before anything is streamed, while
    the body itself is never buffered. A second attempt is rejected at the
    source instead of replaying a drained iterator.
    """

    chunks: tuple[bytes, ...]
    _cursor: int = 0
    _digest: str = field(init=False)

    def __post_init__(self) -> None:
        digest = hashlib.sha256()
        for chunk in self.chunks:
            digest.update(chunk)
        self._digest = digest.hexdigest()

    def __aiter__(self) -> AsyncIterator[bytes]:
        return self

    async def __anext__(self) -> bytes:
        if self._cursor >= len(self.chunks):
            raise StopAsyncIteration
        chunk = self.chunks[self._cursor]
        self._cursor += 1
        return chunk

    @property
    def exhausted(self) -> bool:
        return self._cursor >= len(self.chunks)

    def body_digest(self) -> str:
        return self._digest


def digest_bytes(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()


def compute_signature(
    key: bytes, method: str, target: str, body_digest: str
) -> str:
    message = f"{method.upper()}\n{target}\n{body_digest}".encode()
    return hmac.new(key, message, hashlib.sha256).hexdigest()


def verify_signature(
    key: bytes, method: str, target: str, body_digest: str, signature: str
) -> bool:
    expected = compute_signature(key, method, target, body_digest)
    return hmac.compare_digest(expected, signature)


class OneShotPayload(AsyncIterablePayload):
    """Async payload whose digest is known up front and that cannot replay.

    The digest is computed when the object is built, so the first attempt can
    be signed without buffering. Any attempt to write it a second time raises
    instead of silently streaming whatever the iterator has left.
    """

    def __init__(
        self, value: OneShotAsyncIterable, *args: object, **kwargs: object
    ) -> None:
        super().__init__(value, *args, **kwargs)
        self._known_digest = value.body_digest()
        self._attempts = 0

    async def write_with_length(
        self, writer: AbstractStreamWriter, content_length: int | None
    ) -> None:
        self._attempts += 1
        if self._attempts > 1:
            raise UnreplayableBodyError(
                "one-shot async body was already streamed once; refusing to "
                "send a partial remainder under the original signature"
            )
        await super().write_with_length(writer, content_length)


def is_replayable(body: Payload) -> bool:
    """Whether a second wire attempt can send the same full body again."""
    if isinstance(body, BytesPayload):
        return True
    if isinstance(body, IOBasePayload):
        return not body.consumed
    if isinstance(body, AsyncIterablePayload):
        return False
    return False


async def body_digest_of(payload: Payload) -> str:
    """Digest of the bytes a payload would put on the wire."""
    if isinstance(payload, OneShotPayload):
        return payload._known_digest
    if isinstance(payload, AsyncIterablePayload):
        value = payload._value
        if isinstance(value, OneShotAsyncIterable):
            return value.body_digest()
        # Reading an unknown async iterable would drain it, which would itself
        # make the body unreplayable; refuse rather than consume it implicitly.
        raise UnreplayableBodyError(
            "cannot digest an opaque async iterable without consuming it"
        )
    return digest_bytes(await payload.as_bytes())


@dataclass
class SignatureAudit:
    """What the signing layer saw, safe to print (no key, no HMAC)."""

    middleware_calls: list[tuple[str, str, str]] = field(default_factory=list)
    sequence_numbers: list[int] = field(default_factory=list)

    def render(self) -> list[str]:
        lines = []
        for index, ((method, target, digest), sequence) in enumerate(
            zip(self.middleware_calls, self.sequence_numbers, strict=True)
        ):
            lines.append(
                f"signing-attempt #{index + 1}: method={method} "
                f"target={target} body_sha256={digest[:16]}... "
                f"signature_seq={sequence}"
            )
        return lines


class SigningMiddleware:
    """Signs every attempt and refuses to replay a one-shot body.

    ``reuse_signature_on_retry`` exists only for the negative test that proves
    the audit detects a stale signature. Production code must never set it.
    """

    def __init__(
        self,
        key: bytes = TEST_KEY,
        *,
        reuse_signature_on_retry: bool = False,
    ) -> None:
        self._key = key
        self._reuse_signature_on_retry = reuse_signature_on_retry
        self._sequence = 0
        self.audit = SignatureAudit()

    @property
    def call_count(self) -> int:
        return len(self.audit.middleware_calls)

    def _next_sequence(self) -> int:
        self._sequence += 1
        return self._sequence

    async def sign(
        self,
        request: ClientRequest,
        method: str | None = None,
        target: str | None = None,
        body_digest: str | None = None,
    ) -> int:
        method = request.method if method is None else method
        target = request.url.raw_path_qs if target is None else target
        if body_digest is None:
            body_digest = await body_digest_of(request.body)
        if self._reuse_signature_on_retry and self.audit.sequence_numbers:
            # Deliberately keep the first attempt's target/digest binding.
            first_method, first_target, first_digest = (
                self.audit.middleware_calls[0]
            )
            signature = compute_signature(
                self._key, first_method, first_target, first_digest
            )
            sequence = self.audit.sequence_numbers[0]
        else:
            signature = compute_signature(
                self._key, method, target, body_digest
            )
            sequence = self._next_sequence()
        request.headers[SIGNATURE_HEADER] = signature
        request.headers[BODY_DIGEST_HEADER] = body_digest
        request.headers[SEQUENCE_HEADER] = str(sequence)
        self.audit.middleware_calls.append((method, target, body_digest))
        self.audit.sequence_numbers.append(sequence)
        return sequence

    async def __call__(
        self, request: ClientRequest, handler: ClientHandlerType
    ) -> ClientResponse:
        if self.call_count > 0 and not is_replayable(request.body):
            raise UnreplayableBodyError(
                "a second attempt needs the request body but this payload "
                "cannot be replayed; refusing to resend a partial body under "
                "the original signature"
            )
        await self.sign(request)
        return await handler(request)


class RetryingSigningMiddleware(SigningMiddleware):
    """Signing middleware that performs its own one application-level retry."""

    def __init__(
        self,
        retry_on: Callable[[ClientResponse], bool] | None = None,
        key: bytes = TEST_KEY,
        *,
        reuse_signature_on_retry: bool = False,
    ) -> None:
        super().__init__(key, reuse_signature_on_retry=reuse_signature_on_retry)
        self._retry_on = retry_on or (lambda response: response.status >= 500)

    async def __call__(
        self, request: ClientRequest, handler: ClientHandlerType
    ) -> ClientResponse:
        response: ClientResponse | None = None
        for attempt in range(2):
            if attempt > 0 and not is_replayable(request.body):
                raise UnreplayableBodyError(
                    "middleware retry needs the body again but it is not "
                    "replayable; refusing to resend a partial body under the "
                    "original signature"
                )
            await self.sign(request)
            response = await handler(request)
            if not self._retry_on(response) or attempt == 1:
                return response
            response.release()
        assert response is not None
        return response


def make_redirect_resigning_trace(signer: SigningMiddleware) -> TraceConfig:
    """Re-sign at every redirect hop.

    Client middleware wraps one logical request and therefore runs once even
    when the session follows redirects; this trace hook is the per-wire-request
    point at which the signature can be rebound to the new target. It reuses
    the body digest already computed for the first attempt, which stays
    constant across a 307/308 redirect.
    """

    trace_config = TraceConfig()

    @trace_config.on_request_redirect.append
    async def re_sign(
        session: object,
        ctx: object,
        params: TraceRequestRedirectParams,
    ) -> None:
        location = params.response.headers.get(hdrs.LOCATION)
        if location is None:
            raise UnreplayableBodyError("redirect without a Location header")
        body_digest = params.headers.get(
            BODY_DIGEST_HEADER, _EMPTY_BODY_DIGEST
        )
        new_target = URL(location).raw_path_qs
        sequence = signer._next_sequence()
        params.headers[SIGNATURE_HEADER] = compute_signature(
            signer._key, params.method, new_target, body_digest
        )
        params.headers[BODY_DIGEST_HEADER] = body_digest
        params.headers[SEQUENCE_HEADER] = str(sequence)
        signer.audit.middleware_calls.append(
            (params.method, f"redirect->{new_target}", body_digest)
        )
        signer.audit.sequence_numbers.append(sequence)

    return trace_config
