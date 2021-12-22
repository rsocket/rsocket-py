import asyncio

from reactivestreams.publisher import Publisher
from response_stream import ResponseStream
from rsocket import RSocket, BaseRequestHandler
from rsocket.payload import Payload


class Handler(BaseRequestHandler):
    def request_response(self, payload: Payload) -> asyncio.Future:
        future = asyncio.Future()
        future.set_result(Payload(
            b'The quick brown fox jumps over the lazy dog.',
            b'Escher are an artist.'))
        return future

    def request_stream(self, payload: Payload) -> Publisher:
        return ResponseStream(self.socket)


def session(reader, writer):
    RSocket(reader, writer, handler_factory=Handler)


async def run_server():
    server = await asyncio.start_server(session, 'localhost', 6565)

    async with server:
        await server.serve_forever()


if __name__ == '__main__':
    asyncio.run(run_server())
