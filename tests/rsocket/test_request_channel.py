import asyncio
import logging
from typing import List, Tuple, Optional

from reactivestreams.publisher import Publisher
from reactivestreams.subscriber import DefaultSubscriber, Subscriber
from reactivestreams.subscription import DefaultSubscription, Subscription
from rsocket.payload import Payload
from rsocket.request_handler import BaseRequestHandler, RequestHandler
from rsocket.rsocket_client import RSocketClient
from rsocket.rsocket_server import RSocketServer


async def test_request_channel_properly_finished(pipe: Tuple[RSocketServer, RSocketClient]):
    server, client = pipe
    stream_finished = asyncio.Event()

    class Handler(BaseRequestHandler, Publisher, DefaultSubscription, DefaultSubscriber):
        def cancel(self):
            self.feeder.cancel()

        def subscribe(self, subscriber):
            subscriber.on_subscribe(self)
            self.feeder = asyncio.ensure_future(self.feed(subscriber))

        async def request_channel(self, payload: Payload) -> Tuple[Optional[Publisher], Optional[Subscriber]]:
            return self, self

        @staticmethod
        async def feed(subscriber):
            try:
                for x in range(3):
                    value = Payload('Feed Item: {}'.format(x).encode('utf-8'))
                    subscriber.on_next(value, is_complete=x == 2)
            except asyncio.CancelledError:
                pass

    class StreamSubscriber(DefaultSubscriber):
        def __init__(self):
            self.received_messages: List[Payload] = []

        def on_next(self, value, is_complete=False):
            self.received_messages.append(value)
            logging.info(value)

        def on_complete(self):
            logging.info('Complete')
            stream_finished.set()

        def on_subscribe(self, subscription):
            self.subscription = subscription

    server.set_handler_using_factory(Handler)

    stream_subscriber = StreamSubscriber()

    client.request_channel(Payload(b'')).subscribe(stream_subscriber)

    await stream_finished.wait()

    assert len(stream_subscriber.received_messages) == 3
    assert stream_subscriber.received_messages[0].data == b'Feed Item: 0'
    assert stream_subscriber.received_messages[1].data == b'Feed Item: 1'
    assert stream_subscriber.received_messages[2].data == b'Feed Item: 2'


async def test_request_channel_immediately_finished_without_payloads(pipe: Tuple[RSocketServer, RSocketClient]):
    server, client = pipe
    handler: Optional[RequestHandler] = None
    stream_finished = asyncio.Event()
    response_stream_finished = asyncio.Event()

    class Handler(BaseRequestHandler, Publisher, DefaultSubscription, DefaultSubscriber):

        def __init__(self, socket):
            super().__init__(socket)
            self.received_messages = []

        def on_next(self, value, is_complete=False):
            self.received_messages.append(value)

        def on_complete(self):
            response_stream_finished.set()

        def on_subscribe(self, subscription: Subscription):
            subscription.request(1)

        def subscribe(self, subscriber: Subscriber):
            subscriber.on_subscribe(self)
            self.subscriber = subscriber

        def request(self, n: int):
            self.subscriber.on_complete()

        async def request_channel(self, payload: Payload) -> Tuple[Optional[Publisher], Optional[Subscriber]]:
            return self, self

    class StreamSubscriber(DefaultSubscriber):
        def __init__(self):
            self.received_messages: List[Payload] = []

        def on_next(self, value, is_complete=False):
            self.received_messages.append(value)
            logging.info(value)

        def on_complete(self):
            logging.info('Complete')
            stream_finished.set()

        def on_subscribe(self, subscription: Subscription):
            self.subscription = subscription

    class RequesterPublisher(Publisher, Subscription):

        def request(self, n: int):
            self.subscriber.on_complete()

        def cancel(self):
            pass

        def subscribe(self, subscriber: Subscriber):
            subscriber.on_subscribe(self)
            self.subscriber = subscriber

    def handler_factory(socket):
        nonlocal handler
        handler = Handler(socket)
        return handler

    server.set_handler_using_factory(handler_factory)

    stream_subscriber = StreamSubscriber()

    client.request_channel(Payload(b''), RequesterPublisher()).subscribe(stream_subscriber)

    await stream_finished.wait()
    await response_stream_finished.wait()

    assert len(stream_subscriber.received_messages) == 0
    assert len(handler.received_messages) == 0
