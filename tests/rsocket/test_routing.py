import asyncio
import logging
from typing import List, Tuple

from reactivestreams.publisher import Publisher
from reactivestreams.subscriber import DefaultSubscriber
from reactivestreams.subscription import DefaultSubscription
from rsocket.payload import Payload
from rsocket.routing.helpers import route, composite
from rsocket.routing.request_router import RequestRouter
from rsocket.routing.routing_request_handler import RoutingRequestHandler
from rsocket.rsocket_client import RSocketClient
from rsocket.rsocket_server import RSocketServer


async def test_routed_request_stream_properly_finished(pipe: Tuple[RSocketServer, RSocketClient]):
    server, client = pipe
    stream_finished = asyncio.Event()

    router = RequestRouter()

    def handler_factory(socket):
        return RoutingRequestHandler(socket, router)

    class ResponseStream(Publisher, DefaultSubscription):
        def cancel(self):
            self.feeder.cancel()

        def subscribe(self, subscriber):
            subscriber.on_subscribe(self)
            self.feeder = asyncio.ensure_future(self.feed(subscriber))

        @staticmethod
        async def feed(subscriber):
            loop = asyncio.get_event_loop()
            try:
                for x in range(3):
                    value = Payload('Feed Item: {}'.format(x).encode('utf-8'))
                    subscriber.on_next(value)
                loop.call_soon(subscriber.on_complete)
            except asyncio.CancelledError:
                pass

    @router.stream('test.path')
    async def response_stream(payload, composite_metadata):
        return ResponseStream()

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

    server.set_handler_using_factory(handler_factory)

    stream_subscriber = StreamSubscriber()

    client.request_stream(Payload(b'', composite(route('test.path')))).subscribe(stream_subscriber)

    await stream_finished.wait()

    assert len(stream_subscriber.received_messages) == 3
    assert stream_subscriber.received_messages[0].data == b'Feed Item: 0'
    assert stream_subscriber.received_messages[1].data == b'Feed Item: 1'
    assert stream_subscriber.received_messages[2].data == b'Feed Item: 2'


async def test_routed_request_response_properly_finished(pipe: Tuple[RSocketServer, RSocketClient]):
    server, client = pipe

    router = RequestRouter()

    def handler_factory(socket):
        return RoutingRequestHandler(socket, router)

    @router.response('test.path')
    async def response(payload, composite_metadata):
        future = asyncio.Future()
        future.set_result(Payload(b'result'))
        return future

    server.set_handler_using_factory(handler_factory)

    result = await client.request_response(Payload(b'', composite(route('test.path'))))

    assert result.data == b'result'


async def test_routed_request_channel_properly_finished(pipe: Tuple[RSocketServer, RSocketClient]):
    server, client = pipe
    stream_finished = asyncio.Event()

    router = RequestRouter()

    def handler_factory(socket):
        return RoutingRequestHandler(socket, router)

    class ResponseStream(Publisher, DefaultSubscription, DefaultSubscriber):
        def cancel(self):
            self.feeder.cancel()

        def subscribe(self, subscriber):
            subscriber.on_subscribe(self)
            self.feeder = asyncio.ensure_future(self.feed(subscriber))

        @staticmethod
        async def feed(subscriber):
            loop = asyncio.get_event_loop()
            try:
                for x in range(3):
                    value = Payload('Feed Item: {}'.format(x).encode('utf-8'))
                    subscriber.on_next(value)
                loop.call_soon(subscriber.on_complete)
            except asyncio.CancelledError:
                pass

    @router.channel('test.path')
    async def response_stream(payload, composite_metadata):
        response = ResponseStream()
        return response, response

    class StreamSubscriber(DefaultSubscriber):
        def __init__(self):
            self.received_messages: List[Payload] = []

        def on_next(self, value, is_complete=False):
            self.received_messages.append(value)
            logging.info(value)

        def on_complete(self):
            logging.info('Complete')
            stream_finished.set()

        def on_error(self, exception: Exception):
            stream_finished.set()

        def on_subscribe(self, subscription):
            self.subscription = subscription

    server.set_handler_using_factory(handler_factory)

    stream_subscriber = StreamSubscriber()

    client.request_channel(Payload(b'', composite(route('test.path')))).subscribe(stream_subscriber)

    await stream_finished.wait()

    assert len(stream_subscriber.received_messages) == 3
    assert stream_subscriber.received_messages[0].data == b'Feed Item: 0'
    assert stream_subscriber.received_messages[1].data == b'Feed Item: 1'
    assert stream_subscriber.received_messages[2].data == b'Feed Item: 2'
