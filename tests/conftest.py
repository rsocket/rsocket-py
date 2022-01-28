import asyncio
import functools
import logging
from asyncio.base_events import Server
from contextlib import asynccontextmanager
from datetime import timedelta
from typing import Optional

import pytest
from aiohttp import web
from aiohttp.test_utils import RawTestServer

from rsocket.frame_parser import FrameParser
from rsocket.rsocket_client import RSocketClient
from rsocket.rsocket_server import RSocketServer
from rsocket.transports.tcp import TransportTCP
from rsocket.transports.websocket import websocket_client, TransportWebsocket

logging.basicConfig(level=logging.DEBUG)


@pytest.fixture(params=[
    'tcp', 'websocket'
])
async def lazy_pipe(request, aiohttp_raw_server, unused_tcp_port):
    pipe_factory = get_pipe_factory_by_id(aiohttp_raw_server, request.param)
    yield functools.partial(pipe_factory, unused_tcp_port)


@pytest.fixture(params=[
    'tcp', 'websocket'
])
async def pipe(request, aiohttp_raw_server, unused_tcp_port):
    pipe_factory = get_pipe_factory_by_id(aiohttp_raw_server, request.param)
    async with pipe_factory(unused_tcp_port) as components:
        yield components


def get_pipe_factory_by_id(aiohttp_raw_server, transport_id: str):
    if transport_id == 'tcp':
        return pipe_factory_tcp
    else:
        return functools.partial(pipe_factory_websocket, aiohttp_raw_server)


@asynccontextmanager
async def pipe_factory_tcp(unused_tcp_port, client_arguments=None, server_arguments=None):
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


@pytest.fixture
def aiohttp_raw_server(event_loop, unused_tcp_port):
    servers = []

    async def go(handler, *args, **kwargs):  # type: ignore[no-untyped-def]
        server = RawTestServer(handler, port=unused_tcp_port)
        await server.start_server(**kwargs)
        servers.append(server)
        return server

    yield go

    async def finalize() -> None:
        while servers:
            await servers.pop().close()

    event_loop.run_until_complete(finalize())


def websocket_handler_test_factory(servers, *args, **kwargs):
    async def websocket_handler(request):
        ws = web.WebSocketResponse()
        await ws.prepare(request)
        transport = TransportWebsocket(ws)
        servers.append(RSocketServer(transport, *args, **kwargs))
        await transport.handle_incoming_ws_messages()
        return ws

    return websocket_handler


@asynccontextmanager
async def pipe_factory_websocket(aiohttp_raw_server, unused_tcp_port, client_arguments=None, server_arguments=None):
    servers = []
    await aiohttp_raw_server(websocket_handler_test_factory(servers, **(server_arguments or {})))

    test_overrides = {'keep_alive_period': timedelta(minutes=20)}
    client_arguments = client_arguments or {}
    client_arguments.update(test_overrides)

    async with websocket_client('http://localhost:{}'.format(unused_tcp_port),
                                **client_arguments) as client:
        server = servers[0]
        yield server, client
        server.close()
