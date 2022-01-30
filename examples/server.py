import asyncio
import logging

from reactivestreams.publisher import Publisher
from response_stream import ResponseStream
from rsocket.payload import Payload
from rsocket.request_handler import BaseRequestHandler
from rsocket.rsocket_server import RSocketServer
from rsocket.transports.tcp import TransportTCP


class Handler(BaseRequestHandler):
    async def request_response(self, payload: Payload) -> asyncio.Future:
        future = asyncio.Future()
        future.set_result(Payload(
            b'The quick brown fox jumps over the lazy dog.',
            b'Escher are an artist.'))
        return future

    async def request_stream(self, payload: Payload) -> Publisher:
        return ResponseStream()


def session(*connection):
    RSocketServer(TransportTCP(*connection), handler_factory=Handler)


async def run_server():
    server = await asyncio.start_server(session, 'localhost', 6565)

    async with server:
        await server.serve_forever()


if __name__ == '__main__':
    logging.basicConfig(level=logging.DEBUG)
    asyncio.run(run_server())
