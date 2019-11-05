"""The server."""

import asyncio

from rsocket import RSocket, BaseRequestHandler
from rsocket.payload import Payload
from reactivestreams.publisher import Publisher


class FluxProcessor(Publisher):

    def __init__(self) -> None:
        self.payloads = []

    def next(self, payload):
        self.payloads.append(payload)

    def subscribe(self, subscriber):
        for payload in self.payloads:
            subscriber.on_next(payload)
        subscriber.on_complete()

    @classmethod
    def from_list(cls, payloads):
        processor = FluxProcessor()
        processor.payloads = payloads
        return processor


class Handler(BaseRequestHandler):

    def request_fire_and_forget(self, payload: Payload):
        print("received fir_and_forget: ", payload.data)

    def request_response(self, payload: Payload) -> asyncio.Future:
        future = asyncio.Future()
        future.set_result(Payload(
            b'The quick brown fox jumps over the lazy dog.',
            b'Escher are an artist.'))
        return future

    def request_stream(self, payload: Payload) -> Publisher:
        return FluxProcessor.from_list([Payload(b'data1', b'metadata'), Payload(b'data2', b'metadata')])


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
