# Signing / replay regression tests

These tests pin down what happens to a one-shot request signature when a
single `ClientSession.request()` call results in more than one wire request.

## What is being measured

For every scenario the tests count and cross-check two things:

1. **Signing attempts** — entries into the signing middleware (plus the
   redirect trace re-signing). Each entry records method, target, and the
   SHA-256 of the body it signs.
2. **Wire requests** — requests that actually reached the server, recorded by
   `WireRecorder`.

Each record contains only the target, a body digest/length, and the public
signature sequence number plus a boolean HMAC verification result. The HMAC
value itself and the signing key are never stored or printed.

## Scenarios

| Scenario | What triggers the second wire request | Middleware entries |
| --- | --- | --- |
| `test_auto_retry_stale_connection` | aiohttp retries an idempotent PUT after a pooled connection dies | one per attempt (2) |
| `test_redirect_307_rebinds_signature` / `..._308_...` | 307/308 keeps method and body and follows `Location` | 1 middleware entry + 1 trace re-sign |
| `test_middleware_self_retry` | the signing middleware retries a 503 itself | 2, both inside the middleware |

Each scenario runs with three body shapes:

* `bytes` — always replayable,
* a seekable file (`open(..., "rb")`) — replayable because the payload
  rewinds to the start position before another write,
* a one-shot async iterable wrapped in `OneShotPayload` — not replayable; a
  second attempt raises `UnreplayableBodyError` (or the framework's own
  `ClientPayloadError` on redirects) instead of sending a drained remainder
  under the original signature.

## The bug these tests catch

If a second wire request goes out carrying the first request's signature,
either `assert_wire_signatures_consistent` (server-side HMAC no longer matches
the new target/body) or `assert_distinct_signature_sequences` (sequence number
reused) fails. `test_audit_detects_stale_signature_on_second_wire_request`
and `test_stale_signature_bug_is_detected` exercise the detector directly.

## Running

The logic tests need no sockets and run anywhere:

```sh
PYTHONPATH='.' pytest tests/signing_replay/test_signing_logic.py \
    -o addopts="" --confcutdir=tests/signing_replay
```

The functional tests (`test_signing_replay.py`) start a real local aiohttp
server and require permission to bind a loopback port:

```sh
PYTHONPATH='.' pytest tests/signing_replay -o addopts="" \
    --confcutdir=tests/signing_replay
```

In sandboxes where `bind(2)` is denied (`PermissionError: Operation not
permitted`), the functional tests error at server startup; enable local port
binding (`sandbox.network.allowLocalBinding: true`) to run them.
