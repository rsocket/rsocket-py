import asyncio
from asyncio import Event
from contextlib import asynccontextmanager
from typing import Optional

import pytest

from rsocket.rsocket_base import RSocketBase
from rsocket.transports.aiohttp_websocket import websocket_client, websocket_handler_factory
from tests.rsocket.helpers import assert_no_open_streams


@pytest.fixture
def aiohttp_raw_server(event_loop: asyncio.BaseEventLoop, unused_tcp_port):
    from aiohttp.test_utils import RawTestServer

    servers = []

    async def go(handler):
        server = RawTestServer(handler, port=unused_tcp_port)
        await server.start_server()
        servers.append(server)
        return server

    yield go

    async def finalize() -> None:
        while servers:
            await servers.pop().close()

    event_loop.run_until_complete(finalize())



