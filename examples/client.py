"""A client."""

import asyncio

from reactivesocket import Payload
from reactivesocket import ReactiveSocket
from reactivestreams import Subscriber, Subscription


class StreamSubscriber(Subscriber):
    def on_next(self, value):
        print('RS: {}'.format(value))
        self.subscription.request(1)

    def on_complete(self):
        print('RS: Complete')

    def on_error(self, exception):
        print('RS: error: {}'.format(exception))

    def on_subscribe(self, subscription):
        # noinspection PyAttributeOutsideInit
        self.subscription = subscription


async def download(reader, writer):
    socket = ReactiveSocket(reader, writer, server=False)
    payload = Payload(b'The quick brown fox', b'meta')
    print('RR: {}'.format(await socket.request_response(payload)))
    socket.request_stream(payload).subscribe(StreamSubscriber())
    await asyncio.sleep(0.1)
    await socket.close()
    return payload


if __name__ == '__main__':
    loop = asyncio.get_event_loop()
    try:
        connection = loop.run_until_complete(asyncio.open_connection(
            'localhost', 9898))
        loop.run_until_complete(download(*connection))
    finally:
        loop.close()
