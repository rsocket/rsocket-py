import asyncio

import pytest

from rsocket.exceptions import RSocketTransportError
from rsocket.logger import logger
from rsocket.request_handler import BaseRequestHandler
from rsocket.rsocket_client import RSocketClient
from rsocket.transports.tcp import TransportTCP


@pytest.mark.allow_error_log()
async def test_connection_never_established(unused_tcp_port: int):
    class ClientHandler(BaseRequestHandler):
        async def on_connection_lost(self, rsocket, exception: Exception):
            logger().info('Test Reconnecting')
            await rsocket.reconnect()

    async def transport_provider():
        try:
            for i in range(3):
                client_connection = await asyncio.open_connection('localhost', unused_tcp_port)
                yield TransportTCP(*client_connection)

        except Exception:
            logger().error('Client connection error', exc_info=True)
            raise

    with pytest.raises(RSocketTransportError):
        async with RSocketClient(transport_provider(), handler_factory=ClientHandler):
            await asyncio.sleep(1)
