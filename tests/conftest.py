import asyncio
import functools
import logging
from asyncio.base_events import Server
from contextlib import asynccontextmanager
from typing import Optional

import pytest
from aiohttp.test_utils import RawTestServer
from quart import Quart

from rsocket.frame_parser import FrameParser
from rsocket.rsocket_client import RSocketClient
from rsocket.rsocket_server import RSocketServer
from rsocket.transports.aiohttp_websocket import websocket_client, websocket_handler_factory
from rsocket.transports.quart_websocket import websocket_handler
from rsocket.transports.tcp import TransportTCP

logging.basicConfig(level=logging.DEBUG)

tested_transports = [
    'tcp', 'websocket', 'quart'
]


def pytest_configure(config):
    config.addinivalue_line("markers", "allow_error_log: marks tests which are allowed to have errors in the log")


@pytest.fixture(autouse=True)
def fail_on_error_log(caplog, request):
    marks = [m.name for m in request.node.iter_markers()]

    yield

    if 'allow_error_log' not in marks:
        records = caplog.get_records('call')
        errors = [record for record in records if record.levelno >= logging.ERROR]
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


def get_pipe_factory_by_id(aiohttp_raw_server, transport_id: str):
    if transport_id == 'tcp':
        return pipe_factory_tcp
    if transport_id == 'quart':
        return pipe_factory_quart_websocket
    if transport_id == 'websocket':
        return functools.partial(pipe_factory_websocket, aiohttp_raw_server)


@pytest.fixture
async def pipe_tcp_without_auto_connect(unused_tcp_port):
    async with pipe_factory_tcp(unused_tcp_port, auto_connect_client=False) as components:
        yield components


@asynccontextmanager
async def pipe_factory_tcp(unused_tcp_port, client_arguments=None, server_arguments=None, auto_connect_client=True):
    def session(*connection):
        nonlocal server
        server = RSocketServer(TransportTCP(*connection), **(server_arguments or {}))

    async def start():
        nonlocal service, client
        service = await asyncio.start_server(session, host, port)
        connection = await asyncio.open_connection(host, port)

        nonlocal client_arguments
        # test_overrides = {'keep_alive_period': timedelta(minutes=20)}
        client_arguments = client_arguments or {}
        # client_arguments.update(test_overrides)

        client = RSocketClient(TransportTCP(*connection), **(client_arguments or {}))

        if auto_connect_client:
            client.connect()

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


@asynccontextmanager
async def pipe_factory_websocket(aiohttp_raw_server, unused_tcp_port, client_arguments=None, server_arguments=None):
    server = None

    def store_server(new_server):
        nonlocal server
        server = new_server

    await aiohttp_raw_server(websocket_handler_factory(on_server_create=store_server, **(server_arguments or {})))

    # test_overrides = {'keep_alive_period': timedelta(minutes=20)}
    client_arguments = client_arguments or {}
    # client_arguments.update(test_overrides)

    async with websocket_client('http://localhost:{}'.format(unused_tcp_port),
                                **client_arguments) as client:
        yield server, client
        await server.close()


@asynccontextmanager
async def pipe_factory_quart_websocket(unused_tcp_port, client_arguments=None, server_arguments=None):
    app = Quart(__name__)
    server = None

    def store_server(new_server):
        nonlocal server
        server = new_server

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
        yield server, client
        await server.close()

    try:
        server_task.cancel()
        await server_task
    except asyncio.CancelledError:
        pass
