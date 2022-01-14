import asyncio
import logging
from typing import List

import pytest

from reactivestreams.publisher import Publisher
from reactivestreams.subscriber import DefaultSubscriber
from reactivestreams.subscription import DefaultSubscription
from rsocket.exceptions import RSocketApplicationError
from rsocket.payload import Payload
from rsocket.request_handler import BaseRequestHandler


@pytest.mark.asyncio
async def test_request_stream_not_implemented_by_server(pipe):
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
async def test_request_stream_properly_finished(pipe):
    server, client = pipe
    stream_finished = asyncio.Event()

    class Handler(BaseRequestHandler, Publisher, DefaultSubscription):
        def cancel(self):
            self.feeder.cancel()

        def subscribe(self, subscriber):
            subscriber.on_subscribe(self)
            self.feeder = asyncio.ensure_future(self.feed(subscriber))

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

    class StreamSubscriber(DefaultSubscriber):
        def __init__(self):
            self.received_messages: List[Payload] = []

        async def on_next(self, value, is_complete=False):
            self.received_messages.append(value)
            logging.info(value)

        def on_complete(self, value=None):
            logging.info('Complete')
            stream_finished.set()

        def on_subscribe(self, subscription):
            self.subscription = subscription

    server.set_handler_using_factory(Handler)

    stream_subscriber = StreamSubscriber()

    client.request_stream(Payload(b'')).subscribe(stream_subscriber)

    await stream_finished.wait()

    assert len(stream_subscriber.received_messages) == 4
    assert stream_subscriber.received_messages[0].data == b'Feed Item: 0'
    assert stream_subscriber.received_messages[1].data == b'Feed Item: 1'
    assert stream_subscriber.received_messages[2].data == b'Feed Item: 2'
    assert stream_subscriber.received_messages[3].data == b''


@pytest.mark.asyncio
async def test_request_stream_and_cancel_after_first_message(pipe):
    server, client = pipe
    stream_canceled = asyncio.Event()

    class Handler(BaseRequestHandler, Publisher, DefaultSubscription):
        def cancel(self):
            self.feeder.cancel()
            stream_canceled.set()

        def subscribe(self, subscriber):
            subscriber.on_subscribe(self)
            self.feeder = asyncio.ensure_future(self.feed(subscriber))

        async def request_stream(self, payload: Payload) -> Publisher:
            return self

        @staticmethod
        async def feed(subscriber):
            loop = asyncio.get_event_loop()
            try:
                for x in range(3):
                    value = Payload('Feed Item: {}'.format(x).encode('utf-8'))
                    await asyncio.sleep(1)
                    await subscriber.on_next(value)
                loop.call_soon(subscriber.on_complete)
            except asyncio.CancelledError:
                pass

    class StreamSubscriber(DefaultSubscriber):
        def __init__(self):
            self.received_messages: List[Payload] = []

        async def on_next(self, value, is_complete=False):
            self.received_messages.append(value)
            self.subscription.cancel()
            logging.info(value)

        def on_complete(self, value=None):
            logging.info('Complete')

        def on_subscribe(self, subscription):
            self.subscription = subscription

    server.set_handler_using_factory(Handler)

    stream_subscriber = StreamSubscriber()

    client.request_stream(Payload(b'')).subscribe(stream_subscriber)

    await stream_canceled.wait()

    assert len(stream_subscriber.received_messages) == 1
    assert stream_subscriber.received_messages[0].data == b'Feed Item: 0'


# @pytest.mark.asyncio
# async def test_request_stream_with_back_pressure(pipe):
#     server: RSocketServer = pipe[0]
#     client: RSocketClient = pipe[1]
#
#     stream_canceled = asyncio.Event()
#
#     class Handler(BaseRequestHandler, Publisher, DefaultSubscription):
#         def cancel(self):
#             self.feeder.cancel()
#             stream_canceled.set()
#
#         def subscribe(self, subscriber):
#             subscriber.on_subscribe(self)
#             self.feeder = asyncio.ensure_future(self.feed(subscriber))
#
#         async def request_stream(self, payload: Payload) -> Publisher:
#             return self
#
#         @staticmethod
#         async def feed(subscriber):
#             loop = asyncio.get_event_loop()
#             try:
#                 for x in range(3):
#                     value = Payload('Feed Item: {}'.format(x).encode('utf-8'))
#                     await asyncio.sleep(1)
#                     await subscriber.on_next(value)
#                 loop.call_soon(subscriber.on_complete)
#             except asyncio.CancelledError:
#                 pass
#
#     class StreamSubscriber(DefaultSubscriber):
#         def __init__(self):
#             self.received_messages: List[Payload] = []
#
#         async def on_next(self, value, is_complete=False):
#             self.received_messages.append(value)
#             self.subscription.cancel()
#             logging.info(value)
#
#         def on_complete(self, value=None):
#             logging.info('Complete')
#
#         def on_subscribe(self, subscription):
#             self.subscription = subscription
#
#     server.set_handler_using_factory(Handler)
#
#     stream_subscriber = StreamSubscriber()
#
#     client.request_stream(Payload(b'')).limit_rate(1).subscribe(stream_subscriber)
#
#     await stream_canceled.wait()
#
#     assert len(stream_subscriber.received_messages) == 1
#     assert stream_subscriber.received_messages[0].data == b'Feed Item: 0'
