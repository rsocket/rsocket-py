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


@asynccontextmanager
async def pipe_factory_aiohttp_websocket(aiohttp_raw_server, unused_tcp_port, client_arguments=None,
                                         server_arguments=None):
    server: Optional[RSocketBase] = None
    wait_for_server = Event()

    def store_server(new_server):
        nonlocal server
        server = new_server
        wait_for_server.set()

    await aiohttp_raw_server(websocket_handler_factory(on_server_create=store_server, **(server_arguments or {})))

    # test_overrides = {'keep_alive_period': timedelta(minutes=20)}
    client_arguments = client_arguments or {}
    # client_arguments.update(test_overrides)

    async with websocket_client('http://localhost:{}'.format(unused_tcp_port),
                                **client_arguments) as client:
        await wait_for_server.wait()
        yield server, client

    await server.close()
    assert_no_open_streams(client, server)
