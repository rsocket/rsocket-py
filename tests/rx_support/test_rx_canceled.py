import asyncio
from typing import Tuple, AsyncGenerator, Optional

import pytest
from rx import operators

from reactivestreams.publisher import Publisher
from reactivestreams.subscriber import Subscriber
from rsocket.payload import Payload
from rsocket.request_handler import BaseRequestHandler
from rsocket.rsocket_client import RSocketClient
from rsocket.rsocket_server import RSocketServer
from rsocket.rx_support.rx_rsocket import RxRSocket
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

    rx_client = RxRSocket(client)
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


@pytest.mark.parametrize('take_only_n', (1, 2, 6))
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

    rx_client = RxRSocket(client)

    received_messages = await rx_client.request_channel(Payload(b'request text'),
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
