import asyncio
from typing import Tuple, AsyncGenerator, Optional

import pytest
import rx
from rx import operators
from rx.core.typing import Observer, Scheduler
from rx.disposable import Disposable

from reactivestreams.publisher import Publisher
from reactivestreams.subscriber import Subscriber, DefaultSubscriber
from reactivestreams.subscription import Subscription
from rsocket.payload import Payload
from rsocket.request_handler import BaseRequestHandler
from rsocket.rsocket_client import RSocketClient
from rsocket.rsocket_server import RSocketServer
from rsocket.rx_support.rx_rsocket import RxRSocket
from rsocket.streams.stream_from_async_generator import StreamFromAsyncGenerator


@pytest.mark.parametrize('success_count, request_limit', (
        (0, 2),
        (2, 2),
        (3, 2),
))
async def test_rx_support_request_stream_with_error(pipe: Tuple[RSocketServer, RSocketClient],
                                                    success_count,
                                                    request_limit):
    server, client = pipe

    async def generator() -> AsyncGenerator[Tuple[Payload, bool], None]:
        for x in range(success_count):
            yield Payload('Feed Item: {}'.format(x).encode('utf-8')), False

        raise Exception('Some error from responder')

    class Handler(BaseRequestHandler):
        async def request_stream(self, payload: Payload) -> Publisher:
            return StreamFromAsyncGenerator(generator)

    server.set_handler_using_factory(Handler)

    rx_client = RxRSocket(client)

    with pytest.raises(Exception):
        await rx_client.request_stream(
            Payload(b'request text'),
            request_limit=request_limit
        ).pipe(
            operators.map(lambda payload: payload.data),
            operators.to_list()
        )


@pytest.mark.parametrize('success_count, request_limit', (
        (0, 2),
        (2, 2),
        (3, 2),
))
async def test_rx_support_request_channel_with_error_from_requester(
        pipe: Tuple[RSocketServer, RSocketClient],
        success_count,
        request_limit):
    server, client = pipe
    responder_received_error = asyncio.Event()
    server_received_messages = []
    received_error = None

    class ResponderSubscriber(DefaultSubscriber):

        def on_subscribe(self, subscription: Subscription):
            super().on_subscribe(subscription)
            self.subscription.request(1)

        def on_next(self, value, is_complete=False):
            if len(value.data) > 0:
                server_received_messages.append(value.data)
            self.subscription.request(1)

        def on_error(self, exception: Exception):
            nonlocal received_error
            received_error = exception
            responder_received_error.set()

    async def generator() -> AsyncGenerator[Tuple[Payload, bool], None]:
        for x in range(success_count):
            yield Payload('Feed Item: {}'.format(x).encode('utf-8')), x == success_count - 1

    class Handler(BaseRequestHandler):
        async def request_channel(self, payload: Payload) -> Tuple[Optional[Publisher], Optional[Subscriber]]:
            return StreamFromAsyncGenerator(generator), ResponderSubscriber()

    server.set_handler_using_factory(Handler)

    rx_client = RxRSocket(client)

    def test_observable(observer: Observer, scheduler: Optional[Scheduler]):
        observer.on_error(Exception('Some error'))
        return Disposable()

    await rx_client.request_channel(
        Payload(b'request text'),
        observable=rx.create(test_observable),
        request_limit=request_limit
    ).pipe(
        operators.map(lambda payload: payload.data),
        operators.to_list()
    )

    await responder_received_error.wait()

    assert str(received_error) == 'Some error'
