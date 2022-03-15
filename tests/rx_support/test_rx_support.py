import asyncio
from asyncio import Future
from typing import Tuple, AsyncGenerator, Optional

import rx
from rx import operators

from reactivestreams.publisher import Publisher
from reactivestreams.subscriber import Subscriber, DefaultSubscriber
from reactivestreams.subscription import Subscription
from rsocket.helpers import create_future, DefaultPublisherSubscription
from rsocket.payload import Payload
from rsocket.request_handler import BaseRequestHandler
from rsocket.rsocket_client import RSocketClient
from rsocket.rsocket_server import RSocketServer
from rsocket.rx_support.rx_rsocket import RxRSocket
from rsocket.streams.stream_from_async_generator import StreamFromAsyncGenerator


async def test_rx_support_request_stream_properly_finished(pipe: Tuple[RSocketServer, RSocketClient]):
    server, client = pipe

    async def generator() -> AsyncGenerator[Tuple[Payload, bool], None]:
        for x in range(3):
            yield Payload('Feed Item: {}'.format(x).encode('utf-8')), x == 2

    class Handler(BaseRequestHandler):
        async def request_stream(self, payload: Payload) -> Publisher:
            return StreamFromAsyncGenerator(generator)

    server.set_handler_using_factory(Handler)

    rx_client = RxRSocket(client)
    received_messages = await rx_client.request_stream(Payload(b'request text'),
                                                       request_limit=2).pipe(
        operators.map(lambda payload: payload.data),
        operators.to_list()
    )

    assert len(received_messages) == 3
    assert received_messages[0] == b'Feed Item: 0'
    assert received_messages[1] == b'Feed Item: 1'
    assert received_messages[2] == b'Feed Item: 2'


async def test_rx_support_request_stream_immediate_complete(pipe: Tuple[RSocketServer, RSocketClient]):
    server, client = pipe

    class SimpleCompleted(DefaultPublisherSubscription):

        def request(self, n: int):
            self._subscriber.on_complete()

    class Handler(BaseRequestHandler):
        async def request_stream(self, payload: Payload) -> Publisher:
            return SimpleCompleted()

    server.set_handler_using_factory(Handler)

    rx_client = RxRSocket(client)

    result = await rx_client.request_stream(
        Payload(b'request text'),
        request_limit=2
    ).pipe(
        operators.map(lambda payload: payload.data),
        operators.to_list()
    )

    assert len(result) == 0


async def test_rx_support_request_response_properly_finished(pipe: Tuple[RSocketServer, RSocketClient]):
    server, client = pipe

    class Handler(BaseRequestHandler):
        async def request_response(self, payload: Payload) -> Future:
            return create_future(Payload(b'Response'))

    server.set_handler_using_factory(Handler)

    rx_client = RxRSocket(client)
    received_message = await rx_client.request_response(Payload(b'request text')).pipe(
        operators.map(lambda payload: payload.data),
        operators.single()
    )

    assert received_message == b'Response'


async def test_rx_support_request_channel_properly_finished(pipe: Tuple[RSocketServer, RSocketClient]):
    server, client = pipe
    server_received_messages = []

    responder_received_all = asyncio.Event()

    async def generator() -> AsyncGenerator[Tuple[Payload, bool], None]:
        for x in range(3):
            yield Payload('Feed Item: {}'.format(x).encode('utf-8')), x == 2

    class ResponderSubscriber(DefaultSubscriber):

        def on_subscribe(self, subscription: Subscription):
            super().on_subscribe(subscription)
            self.subscription.request(1)

        def on_next(self, value, is_complete=False):
            if len(value.data) > 0:
                server_received_messages.append(value.data)

            self.subscription.request(1)

        def on_complete(self):
            responder_received_all.set()

    class Handler(BaseRequestHandler):
        async def request_channel(self, payload: Payload) -> Tuple[Optional[Publisher], Optional[Subscriber]]:
            return StreamFromAsyncGenerator(generator), ResponderSubscriber()

    server.set_handler_using_factory(Handler)

    rx_client = RxRSocket(client)

    sent_messages = [b'1', b'2', b'3']
    sent_payloads = [Payload(data) for data in sent_messages]
    received_messages = await rx_client.request_channel(Payload(b'request text'),
                                                        observable=rx.from_list(sent_payloads),
                                                        request_limit=2).pipe(
        operators.map(lambda payload: payload.data),
        operators.to_list()
    )

    await responder_received_all.wait()

    assert server_received_messages == sent_messages

    assert len(received_messages) == 3
    assert received_messages[0] == b'Feed Item: 0'
    assert received_messages[1] == b'Feed Item: 1'
    assert received_messages[2] == b'Feed Item: 2'


async def test_rx_support_request_channel_response_only_properly_finished(pipe: Tuple[RSocketServer, RSocketClient]):
    server, client = pipe

    async def generator() -> AsyncGenerator[Tuple[Payload, bool], None]:
        for x in range(3):
            yield Payload('Feed Item: {}'.format(x).encode('utf-8')), x == 2

    class Handler(BaseRequestHandler):
        async def request_channel(self, payload: Payload) -> Tuple[Optional[Publisher], Optional[Subscriber]]:
            return StreamFromAsyncGenerator(generator), None

    server.set_handler_using_factory(Handler)

    rx_client = RxRSocket(client)

    received_messages = await rx_client.request_channel(Payload(b'request text'),
                                                        request_limit=2).pipe(
        operators.map(lambda payload: payload.data),
        operators.to_list()
    )

    assert len(received_messages) == 3
    assert received_messages[0] == b'Feed Item: 0'
    assert received_messages[1] == b'Feed Item: 1'
    assert received_messages[2] == b'Feed Item: 2'


async def test_rx_rsocket_context_manager(pipe_tcp_without_auto_connect):
    class Handler(BaseRequestHandler):
        async def request_response(self, payload: Payload) -> Future:
            return create_future(Payload(b'Response'))

    server_provider, client = pipe_tcp_without_auto_connect

    async with RxRSocket(client) as rx_client:
        server = await server_provider()
        server.set_handler_using_factory(Handler)

        received_message = await rx_client.request_response(Payload(b'request text')).pipe(
            operators.map(lambda payload: payload.data),
            operators.single()
        )

        assert received_message == b'Response'


async def test_rx_support_metadata_push(pipe: Tuple[RSocketServer, RSocketClient]):
    server, client = pipe
    received_item_event = asyncio.Event()
    received_item = None

    class Handler(BaseRequestHandler):
        async def on_metadata_push(self, payload: Payload):
            nonlocal received_item
            received_item = payload.metadata
            received_item_event.set()

    server.set_handler_using_factory(Handler)

    rx_client = RxRSocket(client)
    rx_client.metadata_push(b'request text')

    await received_item_event.wait()

    assert received_item == b'request text'


async def test_rx_support_fire_and_forget(pipe: Tuple[RSocketServer, RSocketClient]):
    server, client = pipe
    received_item_event = asyncio.Event()
    received_item = None

    class Handler(BaseRequestHandler):
        async def request_fire_and_forget(self, payload: Payload):
            nonlocal received_item
            received_item = payload.data
            received_item_event.set()

    server.set_handler_using_factory(Handler)

    rx_client = RxRSocket(client)
    rx_client.fire_and_forget(Payload(b'request text'))

    await received_item_event.wait()

    assert received_item == b'request text'
