import asyncio
from asyncio import Event, Future
from asyncio.base_events import Server
from datetime import timedelta
from typing import Optional, Tuple

import pytest

from reactivestreams.publisher import Publisher
from rsocket.awaitable.awaitable_rsocket import AwaitableRSocket
from rsocket.error_codes import ErrorCode
from rsocket.exceptions import RSocketProtocolError
from rsocket.frame import Frame
from rsocket.logger import logger
from rsocket.payload import Payload
from rsocket.request_handler import BaseRequestHandler
from rsocket.rsocket_client import RSocketClient
from rsocket.rsocket_server import RSocketServer
from rsocket.streams.stream_from_async_generator import StreamFromAsyncGenerator
from rsocket.transports.tcp import TransportTCP
from rsocket.transports.transport import Transport
from tests.rsocket.helpers import future_from_payload, IdentifiedHandlerFactory, \
    IdentifiedHandler, force_closing_connection


class ServerHandler(IdentifiedHandler):
    def __init__(self, socket, server_id: int, delay=timedelta(0)):
        super().__init__(socket, server_id, delay)
        self._delay = delay

    async def request_response(self, payload: Payload) -> Future:
        await asyncio.sleep(self._delay.total_seconds())
        return future_from_payload(Payload(payload.data + (' server %d' % self._server_id).encode(), payload.metadata))

    async def request_stream(self, payload: Payload) -> Publisher:
        return StreamFromAsyncGenerator(self.feed, delay_between_messages=self._delay)

    async def feed(self):
        for x in range(10):
            value = Payload('Feed Item: {}'.format(x).encode('utf-8'))
            yield value, x == 2


async def test_connection_lost(unused_tcp_port):
    index_iterator = iter(range(1, 3))

    wait_for_server = Event()
    server_connection: Optional[Tuple] = None
    client_connection: Optional[Tuple] = None

    class ClientHandler(BaseRequestHandler):
        async def on_connection_lost(self, rsocket, exception: Exception):
            logger().info('Test Reconnecting')
            await rsocket.reconnect()

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
            while True:
                try:
                    nonlocal client_connection
                    client_connection = await asyncio.open_connection(host, port)
                    yield TransportTCP(*client_connection)
                except Exception:
                    logger().error('Client connection error', exc_info=True)
                    raise

        client = RSocketClient(transport_provider(), handler_factory=ClientHandler)

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
            await force_closing_connection(server_connection)
            await server.close()  # cleanup async tasks from previous server to avoid errors (?)
            await wait_for_server.wait()
            response2 = await connection.request_response(Payload(b'request 2'))

            assert response1.data == b'data: request 1 server 1'
            assert response2.data == b'data: request 2 server 2'
    finally:
        await server.close()

        service.close()


class FailingTransportTCP(Transport):

    async def connect(self):
        raise Exception

    async def send_frame(self, frame: Frame):
        pass

    async def next_frame_generator(self, is_server_alive):
        pass

    async def close(self):
        pass


@pytest.mark.allow_error_log(regex_filter='Connection error')
async def test_connection_failure(unused_tcp_port):
    index_iterator = iter(range(1, 3))

    wait_for_server = Event()
    server_connection: Optional[Tuple] = None
    client_connection: Optional[Tuple] = None

    class ClientHandler(BaseRequestHandler):
        async def on_connection_lost(self, rsocket, exception: Exception):
            logger().info('Test Reconnecting')
            await rsocket.reconnect()

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
                yield TransportTCP(*client_connection)

                yield FailingTransportTCP()

                client_connection = await asyncio.open_connection(host, port)
                yield TransportTCP(*client_connection)
            except Exception:
                logger().error('Client connection error', exc_info=True)
                raise

        client = RSocketClient(transport_provider(), handler_factory=ClientHandler)

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

            await force_closing_connection(server_connection)

            await server.close()  # cleanup async tasks from previous server to avoid errors (?)
            await wait_for_server.wait()

            response2 = await connection.request_response(Payload(b'request 2'))

            assert response1.data == b'data: request 1 server 1'
            assert response2.data == b'data: request 2 server 2'
    finally:
        await server.close()

        service.close()


@pytest.mark.allow_error_log(regex_filter='Connection error')
async def test_connection_failure_during_stream(unused_tcp_port):
    index_iterator = iter(range(1, 3))

    wait_for_server = Event()
    server_connection: Optional[Tuple] = None
    client_connection: Optional[Tuple] = None

    class ClientHandler(BaseRequestHandler):
        async def on_connection_lost(self, rsocket, exception: Exception):
            logger().info('Test Reconnecting')
            await rsocket.reconnect()

    def session(*connection):
        nonlocal server, server_connection
        server_connection = connection
        server = RSocketServer(TransportTCP(*connection),
                               IdentifiedHandlerFactory(next(index_iterator),
                                                        ServerHandler,
                                                        delay=timedelta(seconds=1)).factory)
        wait_for_server.set()

    async def start():
        nonlocal service, client
        service = await asyncio.start_server(session, host, port)

        async def transport_provider():
            try:
                nonlocal client_connection
                client_connection = await asyncio.open_connection(host, port)
                yield TransportTCP(*client_connection)

                yield FailingTransportTCP()

                client_connection = await asyncio.open_connection(host, port)
                yield TransportTCP(*client_connection)
            except Exception:
                logger().error('Client connection error', exc_info=True)
                raise

        client = RSocketClient(transport_provider(), handler_factory=ClientHandler)

    service: Optional[Server] = None
    server: Optional[RSocketServer] = None
    client: Optional[RSocketClient] = None
    port = unused_tcp_port
    host = 'localhost'

    await start()

    try:
        async with AwaitableRSocket(client) as connection:
            await wait_for_server.wait()
            wait_for_server.clear()

            with pytest.raises(RSocketProtocolError) as exc_info:
                await asyncio.gather(
                    connection.request_stream(Payload(b'request 1')),
                    force_closing_connection(server_connection, timedelta(seconds=2)))

            assert exc_info.value.data == 'Connection error'
            assert exc_info.value.error_code == ErrorCode.CONNECTION_ERROR

            await server.close()  # cleanup async tasks from previous server to avoid errors (?)
            await wait_for_server.wait()
            response2 = await connection.request_response(Payload(b'request 2'))

            assert response2.data == b'data: request 2 server 2'
    finally:
        await server.close()

        service.close()
