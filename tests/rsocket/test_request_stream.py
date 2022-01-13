import asyncio
import logging

import pytest

from reactivestreams.publisher import Publisher
from reactivestreams.subscriber import Subscriber, DefaultSubscriber
from reactivestreams.subscription import Subscription
from rsocket.exceptions import RSocketApplicationError
from rsocket.payload import Payload
from rsocket.request_handler import BaseRequestHandler


@pytest.mark.asyncio
async def test_base_request_handler(pipe):
    payload = Payload(b'abc', b'def')
    server, client = pipe

    with pytest.raises(RSocketApplicationError):
        await client.request_response(payload)

    with pytest.raises(RSocketApplicationError):
        class Receiver(DefaultSubscriber):
            def on_error(self, exception):
                error.set_exception(exception)

        error = asyncio.Future()
        client.request_stream(payload).subscribe(Receiver())
        await asyncio.wait_for(error, 0.25)
        not error.done() or error.exception()


@pytest.mark.asyncio
async def test_request_stream(pipe):
    class Handler(BaseRequestHandler, Publisher, Subscription):
        def cancel(self):
            self.feeder.cancel()

        async def request(self, n):
            pass

        def subscribe(self, subscriber):
            subscriber.on_subscribe(self)
            # noinspection PyAttributeOutsideInit
            self.feeder = asyncio.ensure_future(self.feed(subscriber))
            handler_subscribed.set()

        async def request_stream(self, payload: Payload) -> Publisher:
            return self

        @staticmethod
        async def feed(subscriber):
            loop = asyncio.get_event_loop()
            try:
                for x in range(3):
                    value = Payload('Feed Item: {}'.format(x).encode('utf-8'))
                    await subscriber.on_next(value)
                loop.call_soon(subscriber.on_complete)
            except asyncio.CancelledError:
                pass

    class StreamSubscriber(Subscriber):
        async def on_next(self, value, is_complete=False):
            logging.info(value)

        def on_complete(self, value=None):
            logging.info('Complete')

        def on_error(self, exception):
            pass

        def on_subscribe(self, subscription):
            # noinspection PyAttributeOutsideInit
            self.subscription = subscription

    server, client = pipe
    server._handler = handler = Handler(server)
    stream_subscriber = StreamSubscriber()
    publisher = client.request_stream(Payload(b''))

    handler_subscribed = asyncio.Event()
    publisher.subscribe(stream_subscriber)
    await handler_subscribed.wait()

    # TODO: test cancellation and request-n.
    handler.cancel()
