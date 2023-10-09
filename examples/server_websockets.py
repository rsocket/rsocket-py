import asyncio
import logging
import sys
from datetime import datetime

import websockets

from rsocket.helpers import create_future
from rsocket.local_typing import Awaitable
from rsocket.payload import Payload
from rsocket.request_handler import BaseRequestHandler
from rsocket.rsocket_server import RSocketServer
from rsocket.transports.websockets_transport import WebsocketsTransport


class Handler(BaseRequestHandler):
    async def request_response(self, payload: Payload) -> Awaitable[Payload]:
        await asyncio.sleep(0.1)  # Simulate not immediate process
        date_time_format = payload.data.decode('utf-8')
        formatted_date_time = datetime.now().strftime(date_time_format)
        return create_future(Payload(formatted_date_time.encode('utf-8')))


async def endpoint(websocket):
    transport = WebsocketsTransport()
    RSocketServer(transport, handler_factory=Handler)
    await transport.handler(websocket)


async def run_server(server_port):
    logging.info('Starting server at localhost:%s', server_port)

    async with websockets.serve(endpoint, "localhost", server_port):
        await asyncio.Future()  # run forever


if __name__ == '__main__':
    port = sys.argv[1] if len(sys.argv) > 1 else 6565
    logging.basicConfig(level=logging.DEBUG)
    asyncio.run(run_server(port))
