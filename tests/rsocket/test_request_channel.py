import asyncio
from typing import Tuple, Optional

from reactivestreams.publisher import Publisher
from reactivestreams.subscriber import DefaultSubscriber, Subscriber
from reactivestreams.subscription import Subscription
from rsocket.awaitable.awaitable_rsocket import AwaitableRSocket
from rsocket.helpers import DefaultPublisherSubscription
from rsocket.payload import Payload
from rsocket.request_handler import BaseRequestHandler, RequestHandler
from rsocket.rsocket_client import RSocketClient
from rsocket.rsocket_server import RSocketServer
from rsocket.streams.stream_from_generator import StreamFromGenerator


async def test_request_channel_properly_finished(pipe: Tuple[RSocketServer, RSocketClient]):
    server, client = pipe

    def feed():
        for x in range(3):
            yield Payload('Feed Item: {}'.format(x).encode('utf-8')), x == 2

    class Handler(BaseRequestHandler):
        async def request_channel(self, payload: Payload) -> Tuple[Optional[Publisher], Optional[Subscriber]]:
            return StreamFromGenerator(feed), None

    server.set_handler_using_factory(Handler)

    received_messages = await AwaitableRSocket(client).request_channel(Payload())

    assert len(received_messages) == 3
    assert received_messages[0].data == b'Feed Item: 0'
    assert received_messages[1].data == b'Feed Item: 1'
    assert received_messages[2].data == b'Feed Item: 2'


async def test_request_channel_immediately_finished_without_payloads(pipe: Tuple[RSocketServer, RSocketClient]):
    server, client = pipe
    handler: Optional[RequestHandler] = None
    response_stream_finished = asyncio.Event()

    class Handler(BaseRequestHandler, DefaultPublisherSubscription, DefaultSubscriber):

        def __init__(self, socket):
            super().__init__(socket)
            self.received_messages = []

        def on_next(self, value, is_complete=False):
            self.received_messages.append(value)

        def on_complete(self):
            response_stream_finished.set()

        def on_subscribe(self, subscription: Subscription):
            super().on_subscribe(subscription)
            subscription.request(1)

        def request(self, n: int):
            self._subscriber.on_complete()

        async def request_channel(self, payload: Payload) -> Tuple[Optional[Publisher], Optional[Subscriber]]:
            return self, self

    class RequesterPublisher(DefaultPublisherSubscription):

        def request(self, n: int):
            self._subscriber.on_complete()

    def handler_factory(socket):
        nonlocal handler
        handler = Handler(socket)
        return handler

    server.set_handler_using_factory(handler_factory)

    received_messages = await AwaitableRSocket(client).request_channel(Payload(), RequesterPublisher())

    await response_stream_finished.wait()

    assert len(received_messages) == 0
    assert len(handler.received_messages) == 0
