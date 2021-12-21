"""The server."""

import asyncio

from reactivestreams import Publisher, Subscription
from rsocket import RSocket, BaseRequestHandler
from rsocket.payload import Payload


class ResponseStream(BaseRequestHandler, Publisher, Subscription):

    def subscribe(self, subscriber):
        subscriber.on_subscribe(self)
        self.feeder = asyncio.ensure_future(self.feed(subscriber))

    def request(self, n):
        pass

    def cancel(self):
        self.feeder.cancel()

    async def feed(self, subscriber):
        loop = asyncio.get_event_loop()
        try:
            for x in range(3):
                value = Payload('Feed Item: {}'.format(x).encode('utf-8'))
                loop.call_soon(subscriber.on_next, value)
            loop.call_soon(subscriber.on_complete)
        except asyncio.CancelledError:
            pass


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


if __name__ == '__main__':
    loop = asyncio.get_event_loop()
    service = loop.run_until_complete(asyncio.start_server(
        session, 'localhost', 9898))
    try:
        loop.run_forever()
    finally:
        service.close()
        loop.close()
