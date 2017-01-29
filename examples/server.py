"""The server."""

import asyncio

from reactivesocket import ReactiveSocket, BaseRequestHandler
from reactivesocket.payload import Payload


class Handler(BaseRequestHandler):
    def request_response(self, payload: Payload) -> asyncio.Future:
        future = asyncio.Future()
        future.set_result(Payload(
            b'The quick brown fox jumps over the lazy dog.',
            b'Escher are an artist.'))
        return future


def session(reader, writer):
    ReactiveSocket(reader, writer, handler_factory=Handler)


if __name__ == '__main__':
    loop = asyncio.get_event_loop()
    service = loop.run_until_complete(asyncio.start_server(
        session, 'localhost', 9898))
    try:
        loop.run_forever()
    finally:
        service.close()
        loop.close()
