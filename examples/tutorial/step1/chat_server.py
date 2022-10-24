import asyncio

from rsocket.frame_helpers import str_to_bytes
from rsocket.helpers import create_future
from rsocket.local_typing import Awaitable
from rsocket.payload import Payload
from rsocket.request_handler import BaseRequestHandler
from rsocket.rsocket_server import RSocketServer
from rsocket.transports.tcp import TransportTCP


class Handler(BaseRequestHandler):
    async def request_response(self, payload: Payload) -> Awaitable[Payload]:
        return create_future(Payload(str_to_bytes('Welcome to chat')))


async def run_server():
    def session(*connection):
        RSocketServer(TransportTCP(*connection), handler_factory=Handler)

    async with await asyncio.start_server(session, 'localhost', 6565) as server:
        await server.serve_forever()


if __name__ == '__main__':
    asyncio.run(run_server())
