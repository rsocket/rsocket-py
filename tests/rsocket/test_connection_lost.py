import asyncio
from asyncio import Event, Future
from asyncio.base_events import Server
from typing import Optional, Tuple

from rsocket.logger import logger
from rsocket.payload import Payload
from rsocket.request_handler import BaseRequestHandler
from rsocket.rsocket_client import RSocketClient
from rsocket.rsocket_server import RSocketServer
from rsocket.transports.tcp import TransportTCP
from tests.rsocket.helpers import future_from_payload, IdentifiedHandlerFactory, \
    IdentifiedHandler


class ServerHandler(IdentifiedHandler):
    async def request_response(self, payload: Payload) -> Future:
        return future_from_payload(Payload(payload.data + (' server %d' % self._server_id).encode(), payload.metadata))


async def test_connection_lost(unused_tcp_port):
    index_iterator = iter(range(1, 3))

    wait_for_server = Event()
    server_connection: Tuple = None
    client_connection: Tuple = None

    class ClientHandler(BaseRequestHandler):
        async def on_connection_lost(self, rsocket, exception: Exception):
            logger().info('Reconnecting')
            await rsocket.connect()

    def session(*connection):
        nonlocal server, server_connection
        server_connection = connection
        server = RSocketServer(TransportTCP(*connection),
                               IdentifiedHandlerFactory(next(index_iterator), ServerHandler).factory)
        wait_for_server.set()

    async def start():
        nonlocal service, client
        service = await asyncio.start_server(session, host, port)

        async def transport_provider():
            try:
                nonlocal client_connection
                client_connection = await asyncio.open_connection(host, port)
                return TransportTCP(*client_connection)
            except Exception:
                logger().error('Client connection error', exc_info=True)
                raise

        client = RSocketClient(transport_provider, handler_factory=ClientHandler)

    service: Optional[Server] = None
    server: Optional[RSocketServer] = None
    client: Optional[RSocketClient] = None
    port = unused_tcp_port
    host = 'localhost'

    await start()

    try:
        async with client as connection:
            await wait_for_server.wait()
            wait_for_server.clear()
            response1 = await connection.request_response(Payload(b'request 1'))
            force_closing_connection(server_connection)
            await server.close()  # cleanup async tasks from previous server to avoid errors (?)
            await wait_for_server.wait()
            response2 = await connection.request_response(Payload(b'request 2'))

            assert response1.data == b'data: request 1 server 1'
            assert response2.data == b'data: request 2 server 2'
    finally:
        await server.close()

        service.close()


def force_closing_connection(current_connection):
    current_connection[1].close()
