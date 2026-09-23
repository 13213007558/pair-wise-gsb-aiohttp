"""Pool-state snapshots for response release/close/drain semantics.

After a response is cancelled or its context is left early, the
connection must be released or drained to a reusable boundary.  Only a
small, safely drainable tail may be reused; parse/decompression errors
or an early server disconnect must close the connection.
"""

import asyncio
import gzip
from collections.abc import AsyncIterator

import pytest
from pytest_aiohttp import AiohttpClient

import aiohttp
from aiohttp import web
from aiohttp.client_proto import MAX_DRAIN_SIZE
from aiohttp.connector import BaseConnector

BODY = b"The quick brown fox jumps over the lazy dog. " * 20  # 900 bytes
GZ_BODY = gzip.compress(BODY)
BIG_BODY = b"B" * (MAX_DRAIN_SIZE + 1024)


def _pool_snapshot(connector: BaseConnector) -> dict[str, int]:
    return {
        "pool": sum(len(conns) for conns in connector._conns.values()),
        "acquired": len(connector._acquired),
        "draining": len(connector._draining),
    }


async def _length_handler(request: web.Request) -> web.Response:
    return web.Response(body=BODY)


async def _gzip_handler(request: web.Request) -> web.Response:
    return web.Response(body=GZ_BODY, headers={"Content-Encoding": "gzip"})


async def _chunked_handler(request: web.Request) -> web.StreamResponse:
    resp = web.StreamResponse()
    resp.enable_chunked_encoding()
    await resp.prepare(request)
    half = len(BODY) // 2
    await resp.write(BODY[:half])
    # Let the client release before the tail is sent so the connection
    # has to be drained asynchronously.
    await asyncio.sleep(0.1)
    await resp.write(BODY[half:])
    return resp


async def _empty_handler(request: web.Request) -> web.Response:
    return web.Response(status=204)


@pytest.mark.parametrize("handler", [_length_handler, _chunked_handler, _gzip_handler])
async def test_release_drains_small_tail(
    aiohttp_client: AiohttpClient, handler: web.RequestHandler
) -> None:
    """content-length, chunked and compressed responses are drained to a
    reusable boundary when the context is left after a partial read."""
    app = web.Application()
    app.router.add_get("/", handler)
    app.router.add_get("/second", _length_handler)
    client = await aiohttp_client(app)

    async with client.get("/") as resp:
        assert resp.status == 200
        assert await resp.content.read(10) == BODY[:10]
    assert client.session.connector is not None
    connector = client.session.connector

    # The tail is drained in the background; wait for the pool snapshot
    # to settle at the reusable boundary.
    for _ in range(100):
        if _pool_snapshot(connector) == {"pool": 1, "acquired": 0, "draining": 0}:
            break
        await asyncio.sleep(0.01)
    assert _pool_snapshot(connector) == {"pool": 1, "acquired": 0, "draining": 0}
    pooled = next(iter(connector._conns.values()))[0][0]

    async with client.get("/second") as resp2:
        # The next request reuses the drained connection and reads the
        # new response from its first byte, not a stale tail.
        assert resp2.connection is not None
        assert resp2.connection.protocol is pooled
        first = await resp2.content.read(1)
        assert first == BODY[:1]
        assert first + await resp2.read() == BODY


async def test_release_empty_response(aiohttp_client: AiohttpClient) -> None:
    """An empty response is released straight back to the pool."""
    app = web.Application()
    app.router.add_get("/", _empty_handler)
    app.router.add_get("/second", _length_handler)
    client = await aiohttp_client(app)

    async with client.get("/") as resp:
        assert resp.status == 204
    assert client.session.connector is not None
    connector = client.session.connector
    assert _pool_snapshot(connector) == {"pool": 1, "acquired": 0, "draining": 0}
    pooled = next(iter(connector._conns.values()))[0][0]

    async with client.get("/second") as resp2:
        assert resp2.connection is not None
        assert resp2.connection.protocol is pooled
        assert await resp2.read() == BODY


async def test_close_closes_connection_with_small_tail(
    aiohttp_client: AiohttpClient,
) -> None:
    """close() is the forceful variant: even a small tail is not drained."""
    app = web.Application()
    app.router.add_get("/", _chunked_handler)
    client = await aiohttp_client(app)

    resp = await client.get("/")
    await resp.content.read(10)
    resp.close()
    resp.close()  # repeated close is idempotent
    assert resp.closed
    await asyncio.sleep(0.2)
    assert client.session.connector is not None
    assert _pool_snapshot(client.session.connector) == {
        "pool": 0,
        "acquired": 0,
        "draining": 0,
    }


async def test_large_tail_is_closed_not_drained(
    aiohttp_client: AiohttpClient,
) -> None:
    """A tail larger than the drain limit is closed, not drained."""

    async def handler(request: web.Request) -> web.Response:
        return web.Response(body=BIG_BODY)

    app = web.Application()
    app.router.add_get("/", handler)
    client = await aiohttp_client(app)

    async with client.get("/") as resp:
        await resp.content.read(10)
    await asyncio.sleep(0.2)
    assert client.session.connector is not None
    assert _pool_snapshot(client.session.connector) == {
        "pool": 0,
        "acquired": 0,
        "draining": 0,
    }


async def test_decompress_error_closes_connection(
    aiohttp_client: AiohttpClient,
) -> None:
    """A decompression error must close the connection."""

    async def handler(request: web.Request) -> web.Response:
        return web.Response(
            body=b"\x1f\x8b" + b"garbage-" * 16,
            headers={"Content-Encoding": "gzip"},
        )

    app = web.Application()
    app.router.add_get("/", handler)
    client = await aiohttp_client(app)

    resp = await client.get("/")
    with pytest.raises(aiohttp.ClientPayloadError):
        await resp.read()
    await asyncio.sleep(0)
    assert client.session.connector is not None
    assert _pool_snapshot(client.session.connector) == {
        "pool": 0,
        "acquired": 0,
        "draining": 0,
    }


async def test_early_disconnect_closes_connection(
    aiohttp_client: AiohttpClient,
) -> None:
    """A server disconnecting mid-body must close the connection."""

    async def handler(request: web.Request) -> web.StreamResponse:
        resp = web.StreamResponse(headers={"Content-Length": str(len(BODY))})
        await resp.prepare(request)
        await resp.write(BODY[:100])
        assert request.transport is not None
        request.transport.close()
        return resp

    app = web.Application()
    app.router.add_get("/", handler)
    client = await aiohttp_client(app)

    resp = await client.get("/")
    with pytest.raises(aiohttp.ClientPayloadError):
        await resp.read()
    await asyncio.sleep(0)
    assert client.session.connector is not None
    assert _pool_snapshot(client.session.connector) == {
        "pool": 0,
        "acquired": 0,
        "draining": 0,
    }


async def test_cancel_reading_task_closes_connection(
    aiohttp_client: AiohttpClient,
) -> None:
    """Cancelling the reading task leaves no half-consumed connection."""
    app = web.Application()
    app.router.add_get("/", _chunked_handler)
    app.router.add_get("/second", _length_handler)
    client = await aiohttp_client(app)

    async def fetch() -> bytes:
        async with client.get("/") as resp:
            return await resp.read()

    task = asyncio.ensure_future(fetch())
    await asyncio.sleep(0.05)
    task.cancel()
    with pytest.raises(asyncio.CancelledError):
        await task
    await asyncio.sleep(0.2)
    assert client.session.connector is not None
    connector = client.session.connector
    assert _pool_snapshot(connector) == {"pool": 0, "acquired": 0, "draining": 0}

    # The pool is at a clean boundary: the next request reads its own
    # first byte on a fresh connection.
    async with client.get("/second") as resp2:
        first = await resp2.content.read(1)
        assert first == BODY[:1]
        assert first + await resp2.read() == BODY


async def test_release_without_context_manager(
    aiohttp_client: AiohttpClient,
) -> None:
    """release() outside a context manager drains and is idempotent."""
    app = web.Application()
    app.router.add_get("/", _chunked_handler)
    client = await aiohttp_client(app)

    resp = await client.get("/")
    await resp.content.read(10)
    resp.release()
    resp.release()  # repeated release is idempotent
    resp.close()  # close after release is a no-op
    assert resp.closed
    for _ in range(100):
        assert client.session.connector is not None
        if _pool_snapshot(client.session.connector)["draining"] == 0:
            break
        await asyncio.sleep(0.01)
    assert client.session.connector is not None
    assert _pool_snapshot(client.session.connector) == {
        "pool": 1,
        "acquired": 0,
        "draining": 0,
    }


async def test_read_then_close_releases_connection(
    aiohttp_client: AiohttpClient,
) -> None:
    """A fully read response returns the connection; close() stays idempotent."""
    app = web.Application()
    app.router.add_get("/", _length_handler)
    client = await aiohttp_client(app)

    resp = await client.get("/")
    assert await resp.read() == BODY
    resp.close()
    resp.close()
    assert client.session.connector is not None
    assert _pool_snapshot(client.session.connector) == {
        "pool": 1,
        "acquired": 0,
        "draining": 0,
    }


async def test_session_close_with_active_response(
    aiohttp_client: AiohttpClient,
) -> None:
    """session.close() reaps active and draining connections alike."""
    app = web.Application()
    app.router.add_get("/", _chunked_handler)
    client = await aiohttp_client(app)

    resp = await client.get("/")
    await resp.content.read(10)
    resp.release()
    assert client.session.connector is not None
    assert _pool_snapshot(client.session.connector)["draining"] == 1

    await client.session.close()
    assert client.session.connector is None or (
        _pool_snapshot(client.session.connector)
        == {"pool": 0, "acquired": 0, "draining": 0}
    )


async def test_writer_state_after_cancel_with_streaming_body(
    aiohttp_client: AiohttpClient,
) -> None:
    """Cancelling a streaming upload cancels the writer and closes the
    connection; the pool returns to a clean boundary."""

    async def body() -> AsyncIterator[bytes]:
        for _ in range(100):
            yield b"x" * 100
            await asyncio.sleep(0.05)

    async def handler(request: web.Request) -> web.Response:
        return web.Response(body=BODY)

    app = web.Application()
    app.router.add_post("/", handler)
    client = await aiohttp_client(app)

    async def fetch() -> bytes:
        async with client.post("/", data=body()) as resp:
            return await resp.read()

    task = asyncio.ensure_future(fetch())
    await asyncio.sleep(0.2)
    task.cancel()
    with pytest.raises(asyncio.CancelledError):
        await task
    await asyncio.sleep(0.2)
    assert client.session.connector is not None
    assert _pool_snapshot(client.session.connector) == {
        "pool": 0,
        "acquired": 0,
        "draining": 0,
    }


async def test_concurrent_pool_counts_with_drains(
    aiohttp_client: AiohttpClient,
) -> None:
    """Concurrent partial reads all drain back to the pool within the
    connection limit."""
    app = web.Application()
    app.router.add_get("/", _chunked_handler)
    connector = aiohttp.TCPConnector(limit=4)
    client = await aiohttp_client(app, connector=connector)

    async def partial() -> None:
        async with client.get("/") as resp:
            await resp.content.read(10)

    await asyncio.gather(*(partial() for _ in range(4)))
    for _ in range(100):
        assert client.session.connector is not None
        if _pool_snapshot(client.session.connector)["draining"] == 0:
            break
        await asyncio.sleep(0.01)
    assert client.session.connector is not None
    snapshot = _pool_snapshot(client.session.connector)
    assert snapshot["acquired"] == 0
    assert snapshot["draining"] == 0
    assert snapshot["pool"] == 4
