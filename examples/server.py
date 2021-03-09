"""The server."""

import asyncio

import rxbp
from rxbp.schedulers.threadpoolscheduler import ThreadPoolScheduler

from reactivestreams.publisher import Publisher
from rxbp.flowable import Flowable
from rxbp.typing import ElementType

from reactivestreams import Subscriber
from rsocket import RSocket, BaseRequestHandler
from rsocket.payload import Payload


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

    def on_error(self, exception):
        print('Exception: {}'.format(str(exception)))

    def on_completed(self):
        print('Completed!')

    def on_subscribe(self, subscription):
        # noinspection PyAttributeOutsideInit
        self.subscription = subscription
        self.subscription.request(20)


class Handler(BaseRequestHandler):
    def request_response(self, payload: Payload) -> asyncio.Future:
        future = asyncio.Future()
        future.set_result(Payload(
            b'The quick brown fox jumps over the lazy dog.',
            b'Escher are an artist.'))
        return future

    def request_stream(self, payload: Payload) -> Flowable:
        return rxbp.from_list([1, 3, 5, 7, 9], batch_size=1).pipe(
            # rxbp.op.do_action(
            #     on_next=lambda v: print("sending {}".format(v))),
            rxbp.op.map(lambda v: Payload(b'stream-' + str(v).encode('utf-8'), b'm')),
            rxbp.op.subscribe_on(pool),
        )

    def request_channel(self, publisher: Publisher) -> Flowable:
        publisher.subscribe(Observer())
        return rxbp.from_range(10, batch_size=1).pipe(
            # rxbp.op.do_action(
            #     on_next=lambda v: print("sending {}".format(v))),
            rxbp.op.map(lambda v: Payload(b'channel-' + str(v).encode('utf-8'), b'm')),
            rxbp.op.subscribe_on(pool),
        )


def session(reader, writer):
    RSocket(reader, writer, handler_factory=Handler)


if __name__ == '__main__':
    pool = ThreadPoolScheduler("")

    loop = asyncio.get_event_loop()
    service = loop.run_until_complete(asyncio.start_server(
        session, 'localhost', 9898))
    try:
        loop.run_forever()
    finally:
        service.close()
        loop.close()
