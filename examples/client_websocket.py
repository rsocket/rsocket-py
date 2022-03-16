import asyncio
import logging
import sys

from rsocket.payload import Payload
from rsocket.transports.aiohttp_websocket import websocket_client


async def application(serve_port):
    async with websocket_client('http://localhost:%s' % serve_port) as client:
        result = await client.request_response(Payload(b'ping'))
        print(result)


if __name__ == '__main__':
    port = sys.argv[1] if len(sys.argv) > 1 else 6565
    logging.basicConfig(level=logging.DEBUG)
    asyncio.run(application(port))
