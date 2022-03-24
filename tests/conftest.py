import asyncio
import functools
import logging
import re
from asyncio import Event
from asyncio.base_events import Server
from contextlib import asynccontextmanager
import socket
from typing import Optional, Tuple

import pytest

from rsocket.frame_parser import FrameParser
from rsocket.helpers import single_transport_provider
from rsocket.rsocket_base import RSocketBase
from rsocket.rsocket_client import RSocketClient
from rsocket.rsocket_server import RSocketServer
from rsocket.transports.aiohttp_websocket import websocket_client, websocket_handler_factory
from rsocket.transports.aioquic_transport import rsocket_connect, rsocket_serve

from rsocket.transports.tcp import TransportTCP
from tests.rsocket.helpers import assert_no_open_streams

logging.basicConfig(level=logging.DEBUG)

tested_transports = [
    'tcp',
    'aiohttp',
    'quart',
    'quic'
]


def pytest_configure(config):
    config.addinivalue_line("markers", "allow_error_log: marks tests which are allowed to have errors in the log")


@pytest.fixture(autouse=True)
def fail_on_error_log(caplog, request):
    allow_log_error_marker = request.node.get_closest_marker('allow_error_log')

    yield

    def is_allowed_error(record):
        message = record.message
        if allow_log_error_marker is not None:
            if 'regex_filter' in allow_log_error_marker.kwargs:
                regex = re.compile(allow_log_error_marker.kwargs['regex_filter'])
                return regex.search(message) is not None
            return True

        return False

    records = caplog.get_records('call')
    errors = [record for record in records if
              record.levelno >= logging.ERROR and not is_allowed_error(record)]
    assert not errors


@pytest.fixture(params=tested_transports)
async def lazy_pipe(request, aiohttp_raw_server, unused_tcp_port):
    pipe_factory = get_pipe_factory_by_id(aiohttp_raw_server, request.param)
    yield functools.partial(pipe_factory, unused_tcp_port)


@pytest.fixture(params=tested_transports)
async def pipe(request, aiohttp_raw_server, unused_tcp_port):
    pipe_factory = get_pipe_factory_by_id(aiohttp_raw_server, request.param)
    async with pipe_factory(unused_tcp_port) as components:
        yield components


@pytest.fixture
async def pipe_tcp(unused_tcp_port):
    async with pipe_factory_tcp(unused_tcp_port) as components:
        yield components


@pytest.fixture
async def lazy_pipe_tcp(aiohttp_raw_server, unused_tcp_port):
    yield functools.partial(pipe_factory_tcp, unused_tcp_port)


def get_pipe_factory_by_id(aiohttp_raw_server, transport_id: str):
    if transport_id == 'tcp':
        return pipe_factory_tcp
    if transport_id == 'quart':
        return pipe_factory_quart_websocket
    if transport_id == 'aiohttp':
        return functools.partial(pipe_factory_aiohttp_websocket, aiohttp_raw_server)
    if transport_id == 'quic':
        return pipe_factory_quic


@pytest.fixture
async def pipe_tcp_without_auto_connect(unused_tcp_port):
    async with pipe_factory_tcp(unused_tcp_port, auto_connect_client=False) as components:
        yield components


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


@pytest.fixture
def frame_parser():
    return FrameParser()


@pytest.fixture
def aiohttp_raw_server(event_loop: asyncio.BaseEventLoop, unused_tcp_port):
    from aiohttp.test_utils import RawTestServer

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


@asynccontextmanager
async def pipe_factory_quic(unused_tcp_port,
                            client_arguments=None,
                            server_arguments=None):
    server: Optional[RSocketBase] = None
    wait_for_server = Event()

    def store_server(new_server):
        nonlocal server
        server = new_server
        wait_for_server.set()

    quic_server = await rsocket_serve(host='localhost',
                                      port=unused_tcp_port,
                                      on_server_create=store_server,
                                      **(server_arguments or {}))

    # test_overrides = {'keep_alive_period': timedelta(minutes=20)}
    client_arguments = client_arguments or {}
    # client_arguments.update(test_overrides)
    transport = await rsocket_connect('localhost', unused_tcp_port)

    async with RSocketClient(single_transport_provider(transport),
                             **client_arguments) as client:
        await wait_for_server.wait()
        yield server, client
        await server.close()
        assert_no_open_streams(client, server)

    quic_server.cancel()
    await quic_server


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


@asynccontextmanager
async def pipe_factory_quart_websocket(unused_tcp_port, client_arguments=None, server_arguments=None):
    from quart import Quart
    from rsocket.transports.quart_websocket import websocket_handler

    app = Quart(__name__)
    server: Optional[RSocketBase] = None
    wait_for_server = Event()

    def store_server(new_server):
        nonlocal server
        server = new_server
        wait_for_server.set()

    @app.websocket("/")
    async def ws():
        await websocket_handler(on_server_create=store_server, **(server_arguments or {}))
        # test_overrides = {'keep_alive_period': timedelta(minutes=20)}

    client_arguments = client_arguments or {}
    # client_arguments.update(test_overrides)
    server_task = asyncio.create_task(app.run_task(port=unused_tcp_port))
    await asyncio.sleep(0.1)

    async with websocket_client('http://localhost:{}'.format(unused_tcp_port),
                                **client_arguments) as client:
        await wait_for_server.wait()
        yield server, client
        await server.close()
        assert_no_open_streams(client, server)

    try:
        server_task.cancel()
        await server_task
    except asyncio.CancelledError:
        pass


def generate_openssl_certificate_and_key() -> Tuple[str, bytes]:
    import random
    from OpenSSL import crypto

    pkey = crypto.PKey()
    pkey.generate_key(crypto.TYPE_RSA, 2048)

    x509 = crypto.X509()
    subject = x509.get_subject()
    subject.commonName = socket.gethostname()
    x509.set_issuer(subject)
    x509.gmtime_adj_notBefore(0)
    x509.gmtime_adj_notAfter(5 * 365 * 24 * 60 * 60)
    x509.set_pubkey(pkey)
    x509.set_serial_number(random.randrange(100000))
    x509.set_version(2)
    x509.add_extensions([
        crypto.X509Extension(b'subjectAltName', False,
                             ','.join([
                                 'DNS:%s' % socket.gethostname(),
                                 'DNS:*.%s' % socket.gethostname(),
                                 'DNS:localhost',
                                 'DNS:*.localhost']).encode()),
        crypto.X509Extension(b"basicConstraints", True, b"CA:false")])

    x509.sign(pkey, 'SHA256')

    return (crypto.dump_certificate(crypto.FILETYPE_PEM, x509),
            crypto.dump_privatekey(crypto.FILETYPE_PEM, pkey))
