import asyncio
from asyncio import Event
from contextlib import asynccontextmanager
from typing import Optional

import websockets

from rsocket.rsocket_base import RSocketBase
from rsocket.rsocket_server import RSocketServer
from rsocket.transports.aiohttp_websocket import websocket_client
from tests.rsocket.helpers import assert_no_open_streams


@asynccontextmanager
async def pipe_factory_websockets(unused_tcp_port, client_arguments=None, server_arguments=None):
    from rsocket.transports.websockets_transport import WebsocketsTransport

    server: Optional[RSocketBase] = None
    wait_for_server = Event()
    stop_websocket_server = Event()

    async def endpoint(websocket):
        nonlocal server
        transport = WebsocketsTransport()
        server = RSocketServer(transport, **(server_arguments or {}))
        wait_for_server.set()
        await transport.handler(websocket)

    async def server_app():
        async with websockets.serve(endpoint, "localhost", unused_tcp_port):
            await stop_websocket_server.wait()

    server_task = asyncio.create_task(server_app())

    try:
        async with websocket_client('http://localhost:{}'.format(unused_tcp_port),
                                    **(client_arguments or {})) as client:
            await wait_for_server.wait()
            yield server, client

    finally:
        stop_websocket_server.set()
        if server is not None:
            await server.close()

            assert_no_open_streams(client, server)

        try:
            server_task.cancel()
            await server_task
        except asyncio.CancelledError:
            pass
