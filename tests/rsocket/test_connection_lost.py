import asyncio
import logging
from asyncio import Event
from asyncio.base_events import Server
from datetime import timedelta
from typing import Optional, Tuple

import pytest
from aiohttp.test_utils import RawTestServer
from aioquic.quic.configuration import QuicConfiguration
from asyncstdlib import sync
from cryptography.hazmat.primitives import serialization

from reactivestreams.publisher import Publisher
from rsocket.awaitable.awaitable_rsocket import AwaitableRSocket
from rsocket.error_codes import ErrorCode
from rsocket.exceptions import RSocketProtocolError
from rsocket.frame import Frame
from rsocket.local_typing import Awaitable
from rsocket.logger import logger
from rsocket.payload import Payload
from rsocket.request_handler import BaseRequestHandler
from rsocket.rsocket_client import RSocketClient
from rsocket.rsocket_server import RSocketServer
from rsocket.streams.stream_from_async_generator import StreamFromAsyncGenerator
from rsocket.transports.aiohttp_websocket import websocket_handler_factory, TransportAioHttpClient
from rsocket.transports.aioquic_transport import rsocket_connect, rsocket_serve
from rsocket.transports.tcp import TransportTCP
from rsocket.transports.transport import Transport
from tests.rsocket.helpers import future_from_payload, IdentifiedHandlerFactory, \
    IdentifiedHandler, force_closing_connection, ServerContainer


class ServerHandler(IdentifiedHandler):
    def __init__(self, socket, server_id: int, delay=timedelta(0)):
        super().__init__(socket, server_id, delay)
        self._delay = delay

    async def request_response(self, payload: Payload) -> Awaitable[Payload]:
        await asyncio.sleep(self._delay.total_seconds())
        return future_from_payload(Payload(payload.data + (' server %d' % self._server_id).encode(), payload.metadata))

    async def request_stream(self, payload: Payload) -> Publisher:
        return StreamFromAsyncGenerator(self.feed, delay_between_messages=self._delay)

    async def feed(self):
        for x in range(10):
            value = Payload('Feed Item: {}'.format(x).encode('utf-8'))
            yield value, x == 2


async def test_connection_lost(unused_tcp_port):
    logging.info('Testing transport tcp (explicitly) on port %s', unused_tcp_port)

    index_iterator = iter(range(1, 3))

    wait_for_server = Event()
    transport: Optional[Transport] = None
    client_connection: Optional[Tuple] = None

    class ClientHandler(BaseRequestHandler):
        async def on_connection_lost(self, rsocket, exception: Exception):
            logger().info('Test Reconnecting')
            await rsocket.reconnect()

    def session(*connection):
        nonlocal server, transport
        transport = TransportTCP(*connection)
        server = RSocketServer(transport,
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

            await force_closing_connection(transport)

            await server.close()  # cleanup async tasks from previous server to avoid errors (?)
            await wait_for_server.wait()
            response2 = await connection.request_response(Payload(b'request 2'))

            assert response1.data == b'data: request 1 server 1'
            assert response2.data == b'data: request 2 server 2'
    finally:
        await server.close()

        service.close()


class FailingTransport(Transport):

    async def connect(self):
        raise Exception

    async def send_frame(self, frame: Frame):
        pass

    async def next_frame_generator(self):
        pass

    async def close(self):
        pass


@pytest.mark.allow_error_log(regex_filter='Connection error')
async def test_tcp_connection_failure(unused_tcp_port: int):
    logging.info('Testing transport tcp (explicitly) on port %s', unused_tcp_port)

    index_iterator = iter(range(1, 3))

    wait_for_server = Event()
    transport: Optional[Transport] = None
    client_connection: Optional[Tuple] = None

    class ClientHandler(BaseRequestHandler):
        async def on_connection_lost(self, rsocket, exception: Exception):
            logger().info('Test Reconnecting')
            await rsocket.reconnect()

    def session(*connection):
        nonlocal server, transport
        transport = TransportTCP(*connection)
        server = RSocketServer(transport,
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

                yield FailingTransport()

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

            await force_closing_connection(transport)

            await server.close()  # cleanup async tasks from previous server to avoid errors (?)
            await wait_for_server.wait()

            response2 = await connection.request_response(Payload(b'request 2'))

            assert response1.data == b'data: request 1 server 1'
            assert response2.data == b'data: request 2 server 2'
    finally:
        await server.close()

        service.close()


class ClientHandler(BaseRequestHandler):
    async def on_connection_lost(self, rsocket, exception: Exception):
        logger().info('Test Reconnecting')
        await rsocket.reconnect()


async def start_tcp_service(waiter: asyncio.Event, container, port: int, generate_test_certificates):
    index_iterator = iter(range(1, 3))

    def session(*connection):
        container.transport = TransportTCP(*connection)
        container.server = RSocketServer(container.transport,
                                         IdentifiedHandlerFactory(next(index_iterator),
                                                                  ServerHandler,
                                                                  delay=timedelta(seconds=1)).factory)
        waiter.set()

    service = await asyncio.start_server(session, 'localhost', port)
    return sync(service.close)


async def start_tcp_client(port: int, generate_test_certificates) -> RSocketClient:
    async def transport_provider():
        try:
            client_connection = await asyncio.open_connection('localhost', port)
            yield TransportTCP(*client_connection)

            yield FailingTransport()

            client_connection = await asyncio.open_connection('localhost', port)
            yield TransportTCP(*client_connection)
        except Exception:
            logger().error('Client connection error', exc_info=True)
            raise

    return RSocketClient(transport_provider(), handler_factory=ClientHandler)


async def start_websocket_service(waiter: asyncio.Event, container, port: int, generate_test_certificates):
    index_iterator = iter(range(1, 3))

    def handler_factory(*args, **kwargs):
        return IdentifiedHandlerFactory(
            next(index_iterator),
            ServerHandler,
            delay=timedelta(seconds=1)).factory(*args, **kwargs)

    def on_server_create(server):
        container.server = server
        container.transport = server._transport
        waiter.set()

    server = RawTestServer(websocket_handler_factory(on_server_create=on_server_create,
                                                     handler_factory=handler_factory), port=port)
    await server.start_server()
    return server.close


async def start_websocket_client(port: int, generate_test_certificates) -> RSocketClient:
    url = 'http://localhost:{}'.format(port)

    async def transport_provider():
        try:
            yield TransportAioHttpClient(url)

            yield FailingTransport()

            yield TransportAioHttpClient(url)
        except Exception:
            logger().error('Client connection error', exc_info=True)
            raise

    return RSocketClient(transport_provider(), handler_factory=ClientHandler)


async def start_quic_service(waiter: asyncio.Event, container, port: int, generate_test_certificates):
    index_iterator = iter(range(1, 3))
    certificate, private_key = generate_test_certificates
    server_configuration = QuicConfiguration(
        certificate=certificate,
        private_key=private_key,
        is_client=False
    )

    def handler_factory(*args, **kwargs):
        return IdentifiedHandlerFactory(
            next(index_iterator),
            ServerHandler,
            delay=timedelta(seconds=1)).factory(*args, **kwargs)

    def on_server_create(server):
        container.server = server
        container.transport = server._transport
        waiter.set()

    quic_server = await rsocket_serve(host='localhost',
                                      port=port,
                                      configuration=server_configuration,
                                      on_server_create=on_server_create,
                                      handler_factory=handler_factory)
    return sync(quic_server.close)


async def start_quic_client(port: int, generate_test_certificates) -> RSocketClient:
    certificate, private_key = generate_test_certificates
    client_configuration = QuicConfiguration(
        is_client=True
    )
    ca_data = certificate.public_bytes(serialization.Encoding.PEM)
    client_configuration.load_verify_locations(cadata=ca_data, cafile=None)

    async def transport_provider():
        try:
            logging.info('Quic connection lost valid connection 1')
            async with rsocket_connect('localhost', port,
                                       configuration=client_configuration) as transport:
                yield transport

            logging.info('Quic connection lost invalid connection')
            yield FailingTransport()

            logging.info('Quic connection lost valid connection 2')
            async with rsocket_connect('localhost', port,
                                       configuration=client_configuration) as transport:
                yield transport
        except Exception:
            logger().error('Client connection error', exc_info=True)
            raise

    return RSocketClient(transport_provider(), handler_factory=ClientHandler)


@pytest.mark.allow_error_log()  # regex_filter='Connection error') # todo: fix error log
@pytest.mark.parametrize(
    'transport_id, start_service, start_client',
    (
            ('tcp', start_tcp_service, start_tcp_client),
            ('aiohttp', start_websocket_service, start_websocket_client),
            ('quic', start_quic_service, start_quic_client),
    )
)
async def test_connection_failure_during_stream(unused_tcp_port, generate_test_certificates,
                                                transport_id, start_service, start_client):
    logging.info('Testing transport %s on port %s', transport_id, unused_tcp_port)

    server_container = ServerContainer()
    wait_for_server = Event()

    service_closer = await start_service(wait_for_server, server_container, unused_tcp_port, generate_test_certificates)
    client = await start_client(unused_tcp_port, generate_test_certificates)

    try:
        async with AwaitableRSocket(client) as async_client:
            await wait_for_server.wait()
            wait_for_server.clear()

            with pytest.raises(RSocketProtocolError) as exc_info:
                await asyncio.gather(
                    async_client.request_stream(Payload(b'request 1')),
                    force_closing_connection(server_container.transport, timedelta(seconds=2)))

            assert exc_info.value.data == 'Connection error'
            assert exc_info.value.error_code == ErrorCode.CONNECTION_ERROR

            await server_container.server.close()  # cleanup async tasks from previous server to avoid errors (?)
            await wait_for_server.wait()
            response2 = await async_client.request_response(Payload(b'request 2'))

            assert response2.data == b'data: request 2 server 2'
    finally:
        await server_container.server.close()

        await service_closer()
