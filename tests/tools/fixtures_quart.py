import asyncio
from asyncio import Event
from contextlib import asynccontextmanager
from typing import Optional

from rsocket.rsocket_base import RSocketBase
from rsocket.transports.aiohttp_websocket import websocket_client
from tests.rsocket.helpers import assert_no_open_streams


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
