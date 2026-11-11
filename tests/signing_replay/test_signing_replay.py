"""Reproducible tests for one-shot signatures under retries and redirects.

Scenario matrix:

* automatic persistent-connection retry (stale pooled socket, idempotent PUT),
* 307 and 308 redirects that preserve the body,
* an application-level retry performed inside the signing middleware,

each run with bytes, a seekable file, and a one-shot async iterable body.

The tests assert both how many times the signing middleware ran and how many
requests reached the wire, and they fail on the specific bug where a second
wire request is sent under the first request's signature.
"""

import pathlib

import pytest
from pytest_aiohttp import AiohttpServer

from aiohttp import ClientError, ClientSession, TCPConnector

from .audit import (
    AuditFailure,
    WireRecorder,
    assert_distinct_signature_sequences,
    assert_full_bodies,
    assert_middleware_matches_wire,
    assert_wire_signatures_consistent,
)
from .server import build_app
from .signing import (
    OneShotAsyncIterable,
    OneShotPayload,
    RetryingSigningMiddleware,
    SigningMiddleware,
    UnreplayableBodyError,
    digest_bytes,
    make_redirect_resigning_trace,
)

BODY = b"".join(
    [
        b"block-" + bytes((index,)) + b"-payload" * 32
        for index in range(8)
    ]
)
BODY_DIGEST = digest_bytes(BODY)
BODY_LENGTH = len(BODY)


def make_body(kind: str, tmp_path: pathlib.Path):
    if kind == "bytes":
        return BODY, True
    if kind == "file":
        path = tmp_path / "upload.bin"
        path.write_bytes(BODY)
        return open(path, "rb"), True
    if kind == "oneshot":
        source = OneShotAsyncIterable(
            chunks=tuple(BODY[index : index + 64] for index in range(0, len(BODY), 64))
        )
        return OneShotPayload(source), False
    raise AssertionError(f"unknown body kind: {kind}")


BODY_KINDS = ("bytes", "file", "oneshot")


@pytest.fixture
def recorder() -> WireRecorder:
    return WireRecorder()


@pytest.fixture
async def server(aiohttp_server: AiohttpServer, recorder: WireRecorder):
    app = build_app(recorder)
    return await aiohttp_server(app)


def _assert_replayable_outcome(
    records: list,
    signer: SigningMiddleware,
    *,
    expected_wire: int,
    expected_targets: list[str],
    expected_wire_sequences: list[int],
) -> None:
    assert len(records) == expected_wire
    assert_full_bodies(records, BODY_DIGEST, BODY_LENGTH)
    assert_wire_signatures_consistent(records)
    assert_distinct_signature_sequences(records)
    assert [record.target for record in records] == expected_targets
    assert [record.signature_seq for record in records] == expected_wire_sequences
    assert_middleware_matches_wire(expected_wire_sequences, records)


@pytest.mark.parametrize("kind", BODY_KINDS)
async def test_auto_retry_stale_connection(
    server: AiohttpServer,
    recorder: WireRecorder,
    tmp_path: pathlib.Path,
    kind: str,
) -> None:
    body, replayable = make_body(kind, tmp_path)
    signer = SigningMiddleware()
    connector = TCPConnector()
    try:
        async with ClientSession(
            connector=connector, middlewares=(signer,)
        ) as session:
            session._retry_connection = True
            # Prime the pool: the answered connection is returned, then the
            # server drops it so the next signed request reuses a stale socket.
            async with session.post(
                server.make_url("/warmup"), data=b"warm"
            ) as response:
                assert response.status == 200

            if replayable:
                async with session.put(
                    server.make_url("/stale"), data=body
                ) as response:
                    assert response.status == 200
            else:
                with pytest.raises(ClientError):
                    await session.put(server.make_url("/stale"), data=body)
    finally:
        await connector.close()
        if hasattr(body, "close"):
            body.close()

    if replayable:
        # The stale attempt and the rebuilt retry each run the middleware;
        # only the retry reaches the recording handler.
        assert signer.call_count == 2
        assert signer.audit.sequence_numbers == [1, 2]
        _assert_replayable_outcome(
            recorder.records,
            signer,
            expected_wire=1,
            expected_targets=["/stale"],
            expected_wire_sequences=[2],
        )
    else:
        # The failed first attempt never reached the recording handler; no
        # second attempt must be made with a drained body.
        assert recorder.records == []
        assert signer.call_count == 1


@pytest.mark.parametrize("kind", BODY_KINDS)
async def test_redirect_307_rebinds_signature(
    server: AiohttpServer,
    recorder: WireRecorder,
    tmp_path: pathlib.Path,
    kind: str,
) -> None:
    body, replayable = make_body(kind, tmp_path)
    signer = SigningMiddleware()
    trace = make_redirect_resigning_trace(signer)
    try:
        async with ClientSession(
            TCPConnector(), middlewares=(signer,), trace_configs=(trace,)
        ) as session:
            if replayable:
                async with session.put(
                    server.make_url("/redirect307"), data=body
                ) as response:
                    assert response.status == 200
            else:
                with pytest.raises(ClientError):
                    await session.put(
                        server.make_url("/redirect307"), data=body
                    )
    finally:
        if hasattr(body, "close"):
            body.close()

    if replayable:
        # Middleware wraps the single logical request (one entry); the trace
        # performs the second signing at the redirect hop.
        assert signer.call_count == 2
        assert signer.audit.sequence_numbers == [1, 2]
        _assert_replayable_outcome(
            recorder.records,
            signer,
            expected_wire=1,
            expected_targets=["/final"],
            expected_wire_sequences=[2],
        )
    else:
        # The redirect trace re-signs for the new target, then the framework
        # refuses to replay the consumed one-shot body; the final target never
        # receives anything.
        assert recorder.records == []
        assert signer.call_count == 2
        assert signer.audit.sequence_numbers == [1, 2]


@pytest.mark.parametrize("kind", BODY_KINDS)
async def test_redirect_308_rebinds_signature(
    server: AiohttpServer,
    recorder: WireRecorder,
    tmp_path: pathlib.Path,
    kind: str,
) -> None:
    body, replayable = make_body(kind, tmp_path)
    signer = SigningMiddleware()
    trace = make_redirect_resigning_trace(signer)
    try:
        async with ClientSession(
            TCPConnector(), middlewares=(signer,), trace_configs=(trace,)
        ) as session:
            if replayable:
                async with session.put(
                    server.make_url("/redirect308"), data=body
                ) as response:
                    assert response.status == 200
            else:
                with pytest.raises(ClientError):
                    await session.put(
                        server.make_url("/redirect308"), data=body
                    )
    finally:
        if hasattr(body, "close"):
            body.close()

    if replayable:
        assert signer.call_count == 2
        assert signer.audit.sequence_numbers == [1, 2]
        _assert_replayable_outcome(
            recorder.records,
            signer,
            expected_wire=1,
            expected_targets=["/final"],
            expected_wire_sequences=[2],
        )
    else:
        assert recorder.records == []
        assert signer.call_count == 2
        assert signer.audit.sequence_numbers == [1, 2]


@pytest.mark.parametrize("kind", BODY_KINDS)
async def test_middleware_self_retry(
    server: AiohttpServer,
    recorder: WireRecorder,
    tmp_path: pathlib.Path,
    kind: str,
) -> None:
    body, replayable = make_body(kind, tmp_path)
    signer = RetryingSigningMiddleware()
    try:
        async with ClientSession(
            TCPConnector(), middlewares=(signer,)
        ) as session:
            if replayable:
                async with session.put(
                    server.make_url("/flaky"), data=body
                ) as response:
                    assert response.status == 200
            else:
                with pytest.raises(UnreplayableBodyError):
                    await session.put(server.make_url("/flaky"), data=body)
    finally:
        if hasattr(body, "close"):
            body.close()

    if replayable:
        assert signer.call_count == 2
        assert signer.audit.sequence_numbers == [1, 2]
        _assert_replayable_outcome(
            recorder.records,
            signer,
            expected_wire=2,
            expected_targets=["/flaky", "/flaky"],
            expected_wire_sequences=[1, 2],
        )
    else:
        assert len(recorder.records) == 1
        assert_wire_signatures_consistent(recorder.records)
        assert_full_bodies(recorder.records, BODY_DIGEST, BODY_LENGTH)
        assert signer.call_count == 1


def test_audit_detects_stale_signature_on_second_wire_request() -> None:
    """The detector must flag 'second call happened, first content signed'."""
    from .audit import WireRecord

    good = WireRecord(
        attempt=1,
        method="PUT",
        target="/flaky",
        body_sha256=BODY_DIGEST,
        body_length=BODY_LENGTH,
        signature_seq=1,
        signature_valid=True,
    )
    stale = WireRecord(
        attempt=2,
        method="PUT",
        target="/flaky",
        body_sha256=BODY_DIGEST,
        body_length=BODY_LENGTH,
        signature_seq=1,
        signature_valid=False,
    )
    with pytest.raises(AuditFailure):
        assert_wire_signatures_consistent([good, stale])
    with pytest.raises(AuditFailure):
        assert_distinct_signature_sequences([good, stale])
    with pytest.raises(AuditFailure):
        assert_middleware_matches_wire([1, 2], [good, stale])
