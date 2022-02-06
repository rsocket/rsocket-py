import asyncio
import logging

from rsocket.payload import Payload
from rsocket.transports.aiohttp_websocket import websocket_client


async def application():
    async with websocket_client('http://localhost:6565') as client:
        result = await client.request_response(Payload(b'ping'))
        print(result)


if __name__ == '__main__':
    logging.basicConfig(level=logging.DEBUG)
    asyncio.run(application())
