"""A client."""

import asyncio

from reactivestreams import Subscriber
from rsocket import Payload
from rsocket import RSocket
from rsocket.composite_metadata import CompositeMetadata


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
    composite_metadata = CompositeMetadata()
    composite_metadata.add_custom_metadata("application/info", b'{"id":1}')
    socket = RSocket(reader, writer, server=False,
                     data_encoding=b'text/plain', metadata_encoding=b'message/x.rsocket.composite-metadata.v0',
                     setup_payload=Payload(b'', composite_metadata.get_source()))
    payload = Payload(b'The quick brown fox', b'meta')
    print('RR: {}'.format(await socket.request_response(payload)))
    socket.request_stream(payload).subscribe(StreamSubscriber())
    socket.fire_and_forget(Payload(b'fire_and_forget request', b'meta'))
    await asyncio.sleep(1.0)
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
