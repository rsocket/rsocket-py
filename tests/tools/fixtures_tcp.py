import asyncio
from asyncio import Event
from asyncio.base_events import Server
from contextlib import asynccontextmanager
from typing import Optional

from rsocket.helpers import single_transport_provider
from rsocket.rsocket_client import RSocketClient
from rsocket.rsocket_server import RSocketServer
from rsocket.transports.tcp import TransportTCP
from tests.rsocket.helpers import assert_no_open_streams


@asynccontextmanager
async def pipe_factory_tcp(unused_tcp_port, client_arguments=None, server_arguments=None, auto_connect_client=True):
    wait_for_server = Event()

    def session(*connection):
        nonlocal server
        server = RSocketServer(TransportTCP(*connection), **(server_arguments or {}))
        wait_for_server.set()

    async def start():
        nonlocal service, client
        service = await asyncio.start_server(session, host, port)
        connection = await asyncio.open_connection(host, port)
        nonlocal client_arguments
        # test_overrides = {'keep_alive_period': timedelta(minutes=20)}
        client_arguments = client_arguments or {}

        # client_arguments.update(test_overrides)

        client = RSocketClient(single_transport_provider(TransportTCP(*connection)), **(client_arguments or {}))

        if auto_connect_client:
            await client.connect()

    async def finish():
        if auto_connect_client:
            await client.close()

        await server.close()

        service.close()

    service: Optional[Server] = None
    server: Optional[RSocketServer] = None
    client: Optional[RSocketClient] = None
    port = unused_tcp_port
    host = 'localhost'

    await start()

    async def server_provider():
        await wait_for_server.wait()
        return server

    try:
        if auto_connect_client:
            await wait_for_server.wait()
            yield server, client
        else:
            yield server_provider, client

        assert_no_open_streams(client, server)
    finally:
        await finish()
