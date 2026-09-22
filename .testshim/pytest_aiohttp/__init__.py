# Minimal local stand-in for the pytest-aiohttp package (test env only).
from collections.abc import Awaitable, Callable
from typing import Union

from aiohttp.test_utils import RawTestServer, TestClient, TestServer
from aiohttp.web import Application

AiohttpClient = Callable[..., Awaitable[TestClient]]
AiohttpServer = Callable[..., Awaitable[TestServer]]
AiohttpRawServer = Callable[..., Awaitable[RawTestServer]]

__all__ = (
    "AiohttpClient",
    "AiohttpRawServer",
    "AiohttpServer",
)
