"""Binding-free tests for the signing/replay policy.

These tests drive the real middleware with lightweight stand-ins for
``ClientRequest`` and the network handler, so the retry/redirect control flow
can be exercised without binding a local socket (sandboxes that forbid
``bind(2)`` cannot run the functional module). The functional module
(``test_signing_replay.py``) runs the same matrix against a real aiohttp
server wherever local binding is allowed.
"""

import io
from dataclasses import dataclass, field

import pytest
from multidict import CIMultiDict

from aiohttp import ServerDisconnectedError
from aiohttp.payload import PAYLOAD_REGISTRY, BytesPayload

from .audit import (
    AuditFailure,
    WireRecord,
    assert_distinct_signature_sequences,
    assert_full_bodies,
    assert_middleware_matches_wire,
    assert_wire_signatures_consistent,
)
from .signing import (
    BODY_DIGEST_HEADER,
    SEQUENCE_HEADER,
    SIGNATURE_HEADER,
    OneShotAsyncIterable,
    OneShotPayload,
    RetryingSigningMiddleware,
    SigningMiddleware,
    UnreplayableBodyError,
    TEST_KEY,
    body_digest_of,
    compute_signature,
    digest_bytes,
    is_replayable,
    make_redirect_resigning_trace,
    verify_signature,
)

BODY = b"".join(
    [
        b"block-" + bytes((index,)) + b"-payload" * 32
        for index in range(8)
    ]
)
BODY_DIGEST = digest_bytes(BODY)
BODY_LENGTH = len(BODY)


@dataclass
class FakeResponse:
    status: int = 200
    released: bool = False

    @property
    def ok(self) -> bool:
        return 200 <= self.status < 300

    def release(self) -> None:
        self.released = True


@dataclass
class FakeRequest:
    """Minimal ClientRequest surface used by the signing middleware.

    The payload is built once and cached, mirroring ``ClientRequest`` which
    keeps one payload instance across framework-level retries.
    """

    method: str
    target: str
    raw_body: object
    headers: CIMultiDict = field(default_factory=CIMultiDict)
    _cached_payload: object = None

    @property
    def url(self) -> "FakeURL":
        return FakeURL(self.target)

    @property
    def body(self):
        if self._cached_payload is None:
            self._cached_payload = PAYLOAD_REGISTRY.get(
                self.raw_body, disposition=None
            )
        return self._cached_payload


@dataclass
class FakeURL:
    path: str

    @property
    def raw_path_qs(self) -> str:
        return self.path


def make_body(kind: str):
    if kind == "bytes":
        return BODY, True
    if kind == "file":
        return io.BytesIO(BODY), True
    if kind == "oneshot":
        source = OneShotAsyncIterable(
            chunks=tuple(
                BODY[index : index + 64]
                for index in range(0, len(BODY), 64)
            )
        )
        return OneShotPayload(source), False
    raise AssertionError(kind)


class WireSimulator:
    """Stand-in for the network: records what each handler call would send."""

    def __init__(self) -> None:
        self.records: list[WireRecord] = []

    def record_attempt(self, request: FakeRequest) -> None:
        body_digest = request.headers.get(BODY_DIGEST_HEADER, "")
        sequence = int(request.headers[SEQUENCE_HEADER])
        signature = request.headers[SIGNATURE_HEADER]
        valid = verify_signature(
            TEST_KEY,
            request.method,
            request.target,
            body_digest,
            signature,
        )
        self.records.append(
            WireRecord(
                attempt=len(self.records) + 1,
                method=request.method,
                target=request.target,
                body_sha256=body_digest,
                body_length=BODY_LENGTH,
                signature_seq=sequence,
                signature_valid=valid,
            )
        )

    def handler(self, request: FakeRequest) -> FakeResponse:
        self.record_attempt(request)
        return FakeResponse()

    def flaky_handler(self, request: FakeRequest) -> FakeResponse:
        self.record_attempt(request)
        if len(self.records) == 1:
            return FakeResponse(status=503)
        return FakeResponse()

    def disconnect_once_handler(self, request: FakeRequest) -> FakeResponse:
        # First call models the stale pooled socket dying before the handler
        # runs; the outer session loop catches it and re-enters the whole
        # middleware chain with a rebuilt request.
        if not getattr(self, "_disconnect_used", False):
            self._disconnect_used = True
            raise ServerDisconnectedError()
        self.record_attempt(request)
        return FakeResponse()


@pytest.mark.parametrize("kind", ("bytes", "file", "oneshot"))
async def test_framework_auto_retry_invokes_middleware_per_attempt(
    kind: str,
) -> None:
    raw_body, replayable = make_body(kind)
    simulator = WireSimulator()
    signer = SigningMiddleware()
    request = FakeRequest("PUT", "/stale", raw_body)

    async def handler(req: FakeRequest) -> FakeResponse:
        return simulator.disconnect_once_handler(req)

    with pytest.raises(ServerDisconnectedError):
        await signer(request, handler)
    assert signer.call_count == 1
    assert simulator.records == []

    # The session loop rebuilds the request (same cached payload) and re-runs
    # the middleware chain for the retry.
    if replayable:
        response = await signer(request, handler)
        assert response.status == 200
        assert signer.call_count == 2
        assert signer.audit.sequence_numbers == [1, 2]
        assert len(simulator.records) == 1
        assert simulator.records[0].signature_seq == 2
        assert_full_bodies(simulator.records, BODY_DIGEST, BODY_LENGTH)
        assert_wire_signatures_consistent(simulator.records)
    else:
        with pytest.raises(UnreplayableBodyError):
            await signer(request, handler)
        assert simulator.records == []
        assert signer.call_count == 1


@pytest.mark.parametrize("kind", ("bytes", "file", "oneshot"))
async def test_middleware_signs_every_attempt(kind: str) -> None:
    raw_body, replayable = make_body(kind)
    simulator = WireSimulator()
    signer = SigningMiddleware()
    request = FakeRequest("PUT", "/stale", raw_body)

    async def handler(req: FakeRequest) -> FakeResponse:
        return simulator.handler(req)

    response = await signer(request, handler)
    assert response.status == 200
    assert signer.call_count == 1
    assert len(simulator.records) == 1
    assert_wire_signatures_consistent(simulator.records)

    if not replayable:
        # A second attempt must be refused before anything is sent again.
        with pytest.raises(UnreplayableBodyError):
            await signer(request, handler)
        assert len(simulator.records) == 1
        assert signer.call_count == 1
    else:
        response = await signer(request, handler)
        assert response.status == 200
        assert signer.call_count == 2
        assert len(simulator.records) == 2
        assert_full_bodies(simulator.records, BODY_DIGEST, BODY_LENGTH)
        assert_distinct_signature_sequences(simulator.records)
        assert_middleware_matches_wire(
            signer.audit.sequence_numbers, simulator.records
        )


@pytest.mark.parametrize("kind", ("bytes", "file", "oneshot"))
async def test_middleware_self_retry_policy(kind: str) -> None:
    raw_body, replayable = make_body(kind)
    simulator = WireSimulator()
    signer = RetryingSigningMiddleware()
    request = FakeRequest("PUT", "/flaky", raw_body)

    async def handler(req: FakeRequest) -> FakeResponse:
        return simulator.flaky_handler(req)

    if replayable:
        response = await signer(request, handler)
        assert response.status == 200
        assert signer.call_count == 2
        assert len(simulator.records) == 2
        assert_full_bodies(simulator.records, BODY_DIGEST, BODY_LENGTH)
        assert_distinct_signature_sequences(simulator.records)
        assert_middleware_matches_wire(
            signer.audit.sequence_numbers, simulator.records
        )
    else:
        with pytest.raises(UnreplayableBodyError):
            await signer(request, handler)
        assert signer.call_count == 1
        assert len(simulator.records) == 1


async def test_redirect_hop_rebinds_signature_to_new_target() -> None:
    signer = SigningMiddleware()
    trace = make_redirect_resigning_trace(signer)
    request = FakeRequest("PUT", "/redirect307", BODY)

    async def handler(req: FakeRequest) -> FakeResponse:
        return FakeResponse()

    await signer(request, handler)
    first_seq = int(request.headers[SEQUENCE_HEADER])
    first_signature = request.headers[SIGNATURE_HEADER]

    class _RedirectResponse:
        headers = CIMultiDict({"Location": "/final"})

    @dataclass
    class _Params:
        method: str
        headers: CIMultiDict
        response: _RedirectResponse

    signal = trace.on_request_redirect
    signal.freeze()
    params = _Params("PUT", request.headers, _RedirectResponse())
    await signal.send(None, None, params)

    assert int(request.headers[SEQUENCE_HEADER]) == first_seq + 1
    assert request.headers[SIGNATURE_HEADER] != first_signature
    assert verify_signature(
        TEST_KEY,
        "PUT",
        "/final",
        request.headers[BODY_DIGEST_HEADER],
        request.headers[SIGNATURE_HEADER],
    )
    assert signer.call_count == 2


async def test_stale_signature_bug_is_detected() -> None:
    """Second call happens but the first attempt's binding is reused."""
    signer = SigningMiddleware(reuse_signature_on_retry=True)
    simulator = WireSimulator()
    request = FakeRequest("PUT", "/flaky", BODY)

    async def handler(req: FakeRequest) -> FakeResponse:
        return simulator.handler(req)

    await signer(request, handler)
    # Simulate the framework retrying against a different target without
    # re-signing: the buggy middleware keeps the original binding.
    request.target = "/flaky-retry"
    await signer(request, handler)

    assert signer.call_count == 2
    assert signer.audit.sequence_numbers == [1, 1]
    with pytest.raises(AuditFailure):
        assert_wire_signatures_consistent(simulator.records)
    with pytest.raises(AuditFailure):
        assert_distinct_signature_sequences(simulator.records)


async def test_redirect_without_resigning_is_detected() -> None:
    """A redirect hop that keeps the first target's signature must fail."""
    signer = SigningMiddleware()
    request = FakeRequest("PUT", "/redirect307", BODY)

    async def handler(req: FakeRequest) -> FakeResponse:
        return FakeResponse()

    await signer(request, handler)
    first_seq = int(request.headers[SEQUENCE_HEADER])

    # Buggy behavior: follow the redirect but never re-bind the signature.
    stale_record = WireRecord(
        attempt=2,
        method="PUT",
        target="/final",
        body_sha256=request.headers[BODY_DIGEST_HEADER],
        body_length=BODY_LENGTH,
        signature_seq=first_seq,
        signature_valid=verify_signature(
            TEST_KEY,
            "PUT",
            "/final",
            request.headers[BODY_DIGEST_HEADER],
            request.headers[SIGNATURE_HEADER],
        ),
    )
    assert stale_record.signature_valid is False
    with pytest.raises(AuditFailure):
        assert_wire_signatures_consistent([stale_record])
    with pytest.raises(AuditFailure):
        assert_distinct_signature_sequences(
            [
                WireRecord(
                    attempt=1,
                    method="PUT",
                    target="/redirect307",
                    body_sha256=BODY_DIGEST,
                    body_length=BODY_LENGTH,
                    signature_seq=first_seq,
                    signature_valid=True,
                ),
                stale_record,
            ]
        )


async def test_oneshot_payload_cannot_be_written_twice() -> None:
    source = OneShotAsyncIterable(chunks=(b"a", b"b", b"c"))
    payload = OneShotPayload(source)
    assert await body_digest_of(payload) == digest_bytes(b"abc")
    assert not is_replayable(payload)

    class _NullWriter:
        async def write(self, chunk: bytes) -> None:
            pass

    await payload.write_with_length(_NullWriter(), None)
    with pytest.raises(UnreplayableBodyError):
        await payload.write_with_length(_NullWriter(), None)


async def test_bytes_and_file_payloads_are_replayable() -> None:
    assert is_replayable(BytesPayload(BODY))
    file_payload = PAYLOAD_REGISTRY.get(io.BytesIO(BODY), disposition=None)
    assert is_replayable(file_payload)
    assert await body_digest_of(file_payload) == BODY_DIGEST
    assert await body_digest_of(BytesPayload(BODY)) == BODY_DIGEST


async def test_signature_binds_method_target_and_body() -> None:
    base = compute_signature(TEST_KEY, "PUT", "/x", BODY_DIGEST)
    assert verify_signature(TEST_KEY, "PUT", "/x", BODY_DIGEST, base)
    assert not verify_signature(TEST_KEY, "POST", "/x", BODY_DIGEST, base)
    assert not verify_signature(TEST_KEY, "PUT", "/y", BODY_DIGEST, base)
    assert not verify_signature(
        TEST_KEY, "PUT", "/x", digest_bytes(b"other"), base
    )


async def test_audit_records_never_contain_secret() -> None:
    signer = SigningMiddleware()
    request = FakeRequest("PUT", "/x", BODY)

    async def handler(req: FakeRequest) -> FakeResponse:
        return FakeResponse()

    await signer(request, handler)
    rendered = "\n".join(signer.audit.render())
    assert TEST_KEY.decode() not in rendered
    assert request.headers[SIGNATURE_HEADER] not in rendered
