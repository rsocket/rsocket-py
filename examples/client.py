"""A client."""

import asyncio

import rxbp
from rxbp.typing import ElementType

from reactivestreams import Subscriber
from rsocket import Payload
from rsocket import RSocket


# class StreamSubscriber(Subscriber):
#     def on_next(self, value):
#         print('RS: {}'.format(value))
#         self.subscription.request(1)
#
#     def on_complete(self):
#         print('RS: Complete')
#
#     def on_error(self, exception):
#         print('RS: error: {}'.format(exception))
#
#     def on_subscribe(self, subscription):
#         # noinspection PyAttributeOutsideInit
#         self.subscription = subscription


class Observer(Subscriber):
    def on_next(self, elem: ElementType):
        try:
            values = list(elem)
        except Exception as exc:
            print("Exception: ", exc)
        else:
            for v in values:
                data = v.data.decode('utf-8')
                metadata = v.metadata.decode('utf-8')
                print('Response from client: {}, {}'.format(data, metadata))
            self.subscription.request(1)

    def on_error(self, exception):
        print('Exception: {}'.format(str(exception)))

    def on_completed(self):
        print('Completed!')

    def on_subscribe(self, subscription):
        # noinspection PyAttributeOutsideInit
        self.subscription = subscription
        # self.subscription.request(20)


async def download(reader, writer):
    socket = RSocket(reader, writer, server=False)
    payload = Payload(b'The quick brown fox', b'meta')
    print('RR: {}'.format(await socket.request_response(payload)))
    # socket.request_stream(payload).subscribe(StreamSubscriber())

    socket.request_stream(payload).subscribe(Observer())

    socket.request_channel(
        rxbp.from_range(10, batch_size=1).pipe(
            rxbp.op.map(lambda v: Payload(b'channel index-' + str(v).encode('utf-8'), b'm')),
        )
    ).subscribe(Observer())

    await asyncio.sleep(10)
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
