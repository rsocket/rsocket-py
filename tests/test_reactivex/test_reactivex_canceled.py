import asyncio
from asyncio import Event
from typing import Tuple, AsyncGenerator, Optional

import pytest
import reactivex
from reactivex import operators

from reactivestreams.publisher import Publisher
from reactivestreams.subscriber import Subscriber, DefaultSubscriber
from reactivestreams.subscription import Subscription
from rsocket.error_codes import ErrorCode
from rsocket.payload import Payload
from rsocket.request_handler import BaseRequestHandler
from rsocket.rsocket_client import RSocketClient
from rsocket.rsocket_server import RSocketServer
from rsocket.reactivex.reactivex_client import ReactiveXClient
from rsocket.streams.stream_from_async_generator import StreamFromAsyncGenerator


@pytest.mark.parametrize('take_only_n', (
        1,
        2,
        5,
))
async def test_rx_support_request_stream_take_only_n(pipe: Tuple[RSocketServer, RSocketClient],
                                                     take_only_n):
    server, client = pipe
    maximum_message_count = 4
    wait_for_server_finish = asyncio.Event()
    items_generated = 0

    async def generator() -> AsyncGenerator[Tuple[Payload, bool], None]:
        nonlocal items_generated
        for x in range(maximum_message_count):
            items_generated += 1
            yield Payload('Feed Item: {}'.format(x).encode('utf-8')), x == maximum_message_count - 1

    class Handler(BaseRequestHandler):
        async def request_stream(self, payload: Payload) -> Publisher:
            def set_server_finished(): wait_for_server_finish.set()

            return StreamFromAsyncGenerator(generator,
                                            on_cancel=set_server_finished,
                                            on_complete=set_server_finished)

    server.set_handler_using_factory(Handler)

    rx_client = ReactiveXClient(client)
    received_messages = await rx_client.request_stream(Payload(b'request text'),
                                                       request_limit=1).pipe(
        operators.map(lambda payload: payload.data),
        operators.take(take_only_n),
        operators.to_list()
    )

    await wait_for_server_finish.wait()

    maximum_message_received = min(maximum_message_count, take_only_n)

    assert len(received_messages) == maximum_message_received, 'Received message count wrong'
    assert items_generated == maximum_message_received, 'Received message count wrong'

    for i in range(maximum_message_received):
        assert received_messages[i] == ('Feed Item: %d' % i).encode()


@pytest.mark.parametrize('take_only_n', (
        # 0,
        1,
        2,
        6,
))
async def test_rx_support_request_channel_response_take_only_n(pipe: Tuple[RSocketServer, RSocketClient],
                                                               take_only_n):
    server, client = pipe

    maximum_message_count = 4
    wait_for_server_finish = asyncio.Event()
    items_generated = 0

    async def generator() -> AsyncGenerator[Tuple[Payload, bool], None]:
        nonlocal items_generated
        for x in range(maximum_message_count):
            items_generated += 1
            yield Payload('Feed Item: {}'.format(x).encode('utf-8')), x == maximum_message_count - 1

    class Handler(BaseRequestHandler):
        async def request_channel(self, payload: Payload) -> Tuple[Optional[Publisher], Optional[Subscriber]]:
            def set_server_finished(): wait_for_server_finish.set()

            return StreamFromAsyncGenerator(generator,
                                            on_cancel=set_server_finished,
                                            on_complete=set_server_finished), None

    server.set_handler_using_factory(Handler)

    rx_client = ReactiveXClient(client)

    received_messages = await rx_client.request_channel(
        Payload(b'request text'),
        request_limit=1
    ).pipe(
        operators.map(lambda payload: payload.data),
        operators.take(take_only_n),
        operators.to_list()
    )

    if take_only_n > 0:
        await wait_for_server_finish.wait()

    maximum_message_received = min(maximum_message_count, take_only_n)

    assert len(received_messages) == maximum_message_received, 'Received message count wrong'
    assert items_generated == maximum_message_received, 'Received message count wrong'

    for i in range(maximum_message_received):
        assert received_messages[i] == ('Feed Item: %d' % i).encode()


@pytest.mark.parametrize('take_only_n', (
        1,
        2,
        6,
))
async def test_rx_support_request_channel_server_take_only_n(pipe: Tuple[RSocketServer, RSocketClient],
                                                             take_only_n):
    server, client = pipe
    received_messages = []
    items_generated = 0
    maximum_message_count = 3
    responder_receiving_done = asyncio.Event()

    class Handler(BaseRequestHandler, DefaultSubscriber):

        def on_next(self, value: Payload, is_complete=False):
            received_messages.append(value)
            if len(received_messages) < take_only_n:
                self.subscription.request(1)
            else:
                self.subscription.cancel()
                responder_receiving_done.set()

        def on_complete(self):
            responder_receiving_done.set()

        async def on_error(self, error_code: ErrorCode, payload: Payload):
            responder_receiving_done.set()

        def on_subscribe(self, subscription: Subscription):
            super().on_subscribe(subscription)
            subscription.request(1)

        async def request_channel(self, payload: Payload) -> Tuple[Optional[Publisher], Optional[Subscriber]]:
            return None, self

    server.set_handler_using_factory(Handler)

    rx_client = ReactiveXClient(client)

    def generator():
        nonlocal items_generated
        for x in range(maximum_message_count):
            items_generated += 1
            yield Payload('Feed Item: {}'.format(x).encode('utf-8'))

    requester_sending_done = Event()

    await rx_client.request_channel(
        Payload(b'request text'),
        observable=reactivex.from_iterable(generator()),
        sending_done=requester_sending_done
    ).pipe(
        operators.to_list()
    )

    await responder_receiving_done.wait()
    await requester_sending_done.wait()

    maximum_message_received = min(maximum_message_count, take_only_n)

    # assert items_generated == maximum_message_received # todo: Stop async generator on cancel from server requester

    assert len(received_messages) == maximum_message_received

    for i in range(maximum_message_received):
        assert received_messages[i].data == ('Feed Item: %d' % i).encode()
