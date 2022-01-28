import asyncio

from rsocket.payload import Payload
from rsocket.transports.websocket import websocket_client


async def application():
    async with websocket_client('http://localhost:6565') as client:
        result = await client.request_response(Payload(b'ping'))
        print(result)


asyncio.run(application())
