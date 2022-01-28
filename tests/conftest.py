import asyncio
import functools
import logging
from asyncio.base_events import Server
from contextlib import asynccontextmanager
from typing import Optional

import pytest

from rsocket.frame_parser import FrameParser
from rsocket.rsocket_client import RSocketClient
from rsocket.rsocket_server import RSocketServer
from rsocket.transports.tcp import TransportTCP

logging.basicConfig(level=logging.DEBUG)


@pytest.fixture
async def lazy_pipe(unused_tcp_port):
    yield functools.partial(pipe_factory, unused_tcp_port)


@pytest.fixture
async def pipe(unused_tcp_port, event_loop):
    async with pipe_factory(unused_tcp_port) as components:
        yield components


@asynccontextmanager
async def pipe_factory(unused_tcp_port, client_arguments=None, server_arguments=None):
    def session(*connection):
        nonlocal server
        server = RSocketServer(TransportTCP(*connection), **(server_arguments or {}))

    async def start():
        nonlocal service, client
        service = await asyncio.start_server(session, host, port)
        connection = await asyncio.open_connection(host, port)
        client = await RSocketClient(TransportTCP(*connection), **(client_arguments or {})).connect()

    async def finish():
        service.close()
        await client.close()
        await server.close()

    service: Optional[Server] = None
    server: Optional[RSocketServer] = None
    client: Optional[RSocketClient] = None
    port = unused_tcp_port
    host = 'localhost'

    await start()
    try:
        yield server, client
    finally:
        await finish()


@pytest.fixture
def connection():
    return FrameParser()
