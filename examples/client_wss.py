import asyncio
import logging
import sys

import aiohttp

from rsocket.helpers import single_transport_provider
from rsocket.payload import Payload
from rsocket.rsocket_client import RSocketClient
from rsocket.transports.aiohttp_websocket import TransportAioHttpClient


async def application(serve_port):
    async with aiohttp.ClientSession() as session:
        async with session.ws_connect('wss://localhost:%s' % serve_port, verify_ssl=False) as websocket:
            async with RSocketClient(single_transport_provider(TransportAioHttpClient(websocket=websocket))) as client:
                result = await client.request_response(Payload(b'ping'))
                print(result)


if __name__ == '__main__':
    port = sys.argv[1] if len(sys.argv) > 1 else 6565
    logging.basicConfig(level=logging.DEBUG)
    asyncio.run(application(port))
