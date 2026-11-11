"""Server-side wire recording and audit checks.

Each record describes one request actually received on the wire. Records keep
the request target, a SHA-256 digest and length of the body, and the public
signature sequence number. The HMAC value and the signing key are never
stored; the HMAC is verified in place and only the boolean result is kept.
"""

from dataclasses import dataclass, field

from aiohttp import web

from .signing import (
    BODY_DIGEST_HEADER,
    SEQUENCE_HEADER,
    SIGNATURE_HEADER,
    TEST_KEY,
    digest_bytes,
    verify_signature,
)


@dataclass
class WireRecord:
    attempt: int
    method: str
    target: str
    body_sha256: str
    body_length: int
    signature_seq: int | None
    signature_valid: bool

    def render(self) -> str:
        return (
            f"wire-attempt #{self.attempt}: method={self.method} "
            f"target={self.target} body_sha256={self.body_sha256[:16]}... "
            f"body_len={self.body_length} signature_seq={self.signature_seq} "
            f"signature_valid={self.signature_valid}"
        )


@dataclass
class WireRecorder:
    """aiohttp handler factory that records every received wire request."""

    records: list[WireRecord] = field(default_factory=list)

    async def record(self, request: web.Request) -> web.Response:
        body = await request.read()
        body_digest = digest_bytes(body)
        signature = request.headers.get(SIGNATURE_HEADER, "")
        advertised_digest = request.headers.get(
            BODY_DIGEST_HEADER, body_digest
        )
        raw_seq = request.headers.get(SEQUENCE_HEADER)
        sequence = int(raw_seq) if raw_seq is not None else None
        valid = verify_signature(
            TEST_KEY,
            request.method,
            request.url.raw_path_qs,
            body_digest,
            signature,
        ) and advertised_digest == body_digest
        self.records.append(
            WireRecord(
                attempt=len(self.records) + 1,
                method=request.method,
                target=request.url.raw_path_qs,
                body_sha256=body_digest,
                body_length=len(body),
                signature_seq=sequence,
                signature_valid=valid,
            )
        )
        return web.Response(text="ok")

    def render_all(self) -> list[str]:
        return [record.render() for record in self.records]


class AuditFailure(AssertionError):
    """A signing/replay invariant was violated."""


def assert_wire_signatures_consistent(records: list[WireRecord]) -> None:
    """Every wire request must carry a signature matching its own content."""
    for record in records:
        if not record.signature_valid:
            raise AuditFailure(
                "a wire request arrived with a signature that does not match "
                f"its method/target/body: {record.render()}"
            )


def assert_full_bodies(
    records: list[WireRecord], expected_digest: str, expected_length: int
) -> None:
    """Every wire request must carry the whole body, never a remainder."""
    for record in records:
        if record.body_sha256 != expected_digest:
            raise AuditFailure(
                "wire body digest differs from the signed body; the request "
                f"may have resent only the unread remainder: {record.render()}"
            )
        if record.body_length != expected_length:
            raise AuditFailure(
                f"wire body length {record.body_length} != expected "
                f"{expected_length}: {record.render()}"
            )


def assert_distinct_signature_sequences(records: list[WireRecord]) -> None:
    """Multiple wire requests must not reuse one signature sequence number."""
    sequences = [record.signature_seq for record in records]
    if len(sequences) != len(set(sequences)):
        raise AuditFailure(
            "the same signature sequence number was reused across separate "
            f"wire requests: {[record.render() for record in records]}"
        )


def assert_middleware_matches_wire(
    expected_sequences: list[int], records: list[WireRecord]
) -> None:
    """The wire requests must carry exactly the expected signature numbers."""
    wire_sequences = [record.signature_seq for record in records]
    if expected_sequences != wire_sequences:
        raise AuditFailure(
            "signing-layer sequence numbers do not match wire requests: "
            f"expected={expected_sequences} wire={wire_sequences}"
        )
