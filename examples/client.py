"""A client."""

import asyncio
from asyncio import Event

from reactivestreams import Subscriber
from rsocket import Payload
from rsocket import RSocket


class StreamSubscriber(Subscriber):

    def __init__(self, wait_for_complete: Event):
        self._wait_for_complete = wait_for_complete

    def on_next(self, value):
        print('RS: {}'.format(value))
        self.subscription.request(1)

    def on_complete(self):
        print('RS: Complete')
        self._wait_for_complete.set()

    def on_error(self, exception):
        print('RS: error: {}'.format(exception))
        self._wait_for_complete.set()

    def on_subscribe(self, subscription):
        # noinspection PyAttributeOutsideInit
        self.subscription = subscription


async def download(reader, writer):
    socket = RSocket(reader, writer, server=False)

    payload = Payload(b'The quick brown fox', b'meta')

    result = await socket.request_response(payload)
    print('RR: {}'.format(result))

    completion_event = Event()
    socket.request_stream(payload).subscribe(StreamSubscriber(completion_event))

    await completion_event.wait()

    await socket.close()


if __name__ == '__main__':
    loop = asyncio.get_event_loop()
    try:
        connection = loop.run_until_complete(asyncio.open_connection(
            'localhost', 9898))
        loop.run_until_complete(download(*connection))
    finally:
        loop.close()
