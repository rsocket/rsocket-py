import asyncio

import pytest

from rsocket.rsocket_client import RSocketClient
from rsocket.rsocket_server import RSocketServer


@pytest.fixture
def pipe(unused_tcp_port, event_loop):
    def session(reader, writer):
        nonlocal server
        server = RSocketServer(reader, writer)

    async def start():
        nonlocal service, client
        service = await asyncio.start_server(session, host, port)
        connection = await asyncio.open_connection(host, port)
        client = RSocketClient(*connection)

    async def finish():
        service.close()
        await client.close()
        await server.close()

    service, server, client = None, None, None
    port = unused_tcp_port
    host = 'localhost'

    event_loop.run_until_complete(start())
    yield server, client
    event_loop.run_until_complete(finish())
