import asyncio

import pytest


@pytest.fixture
def aiohttp_raw_server(event_loop: asyncio.BaseEventLoop, unused_tcp_port):
    from aiohttp.test_utils import RawTestServer

    servers = []

    try:
        async def go(handler):
            server = RawTestServer(handler, port=unused_tcp_port)
            await server.start_server()
            servers.append(server)
            return server

        yield go
    finally:
        async def finalize() -> None:
            while servers:
                await servers.pop().close()

        event_loop.run_until_complete(finalize())
