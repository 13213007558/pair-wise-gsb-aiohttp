# Minimal local stand-in for pytest-aiohttp's plugin (test env only).
# Provides the small fixture surface used by this repo's test suite:
# aiohttp_unused_port, aiohttp_server, aiohttp_raw_server, aiohttp_client.
import socket
from collections.abc import AsyncIterator, Awaitable, Callable
from typing import Any

import pytest

from aiohttp.test_utils import RawTestServer, TestClient, TestServer
from aiohttp.web import Application


def unused_port() -> int:
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
        sock.bind(("127.0.0.1", 0))
        return sock.getsockname()[1]


@pytest.fixture
def aiohttp_unused_port() -> Callable[[], int]:
    return unused_port


@pytest.fixture
async def aiohttp_server() -> AsyncIterator[Callable[..., Awaitable[TestServer]]]:
    servers = []

    async def go(app: Application, **kwargs: Any) -> TestServer:
        server = TestServer(app, **kwargs)
        await server.start_server()
        servers.append(server)
        return server

    yield go
    for server in servers:
        await server.close()


@pytest.fixture
async def aiohttp_raw_server() -> AsyncIterator[Callable[..., Awaitable[RawTestServer]]]:
    servers = []

    async def go(handler: Any, **kwargs: Any) -> RawTestServer:
        server = RawTestServer(handler, **kwargs)
        await server.start_server()
        servers.append(server)
        return server

    yield go
    for server in servers:
        await server.close()


@pytest.fixture
async def aiohttp_client() -> AsyncIterator[Callable[..., Awaitable[TestClient]]]:
    clients = []

    async def go(app: Application, **kwargs: Any) -> TestClient:
        client = TestClient(TestServer(app), **kwargs)
        await client.start_server()
        clients.append(client)
        return client

    yield go
    for client in clients:
        await client.close()
