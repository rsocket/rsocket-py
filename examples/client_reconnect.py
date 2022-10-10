import asyncio
import logging
import sys

from rsocket.extensions.helpers import route, composite, authenticate_simple
from rsocket.extensions.mimetypes import WellKnownMimeTypes
from rsocket.payload import Payload
from rsocket.request_handler import BaseRequestHandler
from rsocket.rsocket_client import RSocketClient
from rsocket.transports.tcp import TransportTCP


async def request_response(client: RSocketClient) -> Payload:
    payload = Payload(b'The quick brown fox', composite(
        route('single_request'),
        authenticate_simple('user', '12345')
    ))

    return await client.request_response(payload)


class Handler(BaseRequestHandler):

    async def on_connection_lost(self, rsocket: RSocketClient, exception: Exception):
        await asyncio.sleep(5)
        await rsocket.reconnect()


async def main(server_port):
    logging.info('Connecting to server at localhost:%s', server_port)

    async def transport_provider(max_reconnect):
        for i in range(max_reconnect):
            connection = await asyncio.open_connection('localhost', server_port)
            yield TransportTCP(*connection)

    async with RSocketClient(transport_provider(3),
                             metadata_encoding=WellKnownMimeTypes.MESSAGE_RSOCKET_COMPOSITE_METADATA,
                             handler_factory=Handler) as client:
        result1 = await request_response(client)
        assert result1.data == b'single_response'

        await asyncio.sleep(10)

        result2 = await request_response(client)
        assert result2.data == b'single_response'

        result3 = await request_response(client)
        assert result3.data == b'single_response'


if __name__ == '__main__':
    port = sys.argv[1] if len(sys.argv) > 1 else 6565
    logging.basicConfig(level=logging.DEBUG)
    asyncio.run(main(port))
