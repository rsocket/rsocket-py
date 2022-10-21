import asyncio
from asyncio import Future
from typing import Tuple

import pytest
from rx import operators

from reactivestreams.publisher import Publisher
from rsocket.helpers import create_future, DefaultPublisherSubscription
from rsocket.payload import Payload
from rsocket.request_handler import BaseRequestHandler
from rsocket.rsocket_client import RSocketClient
from rsocket.rsocket_server import RSocketServer
from rsocket.rx_support.rx_rsocket import RxRSocket
from tests.rsocket.helpers import get_components


async def test_rx_support_request_stream_cancel_on_timeout(pipe: Tuple[RSocketServer, RSocketClient]):
    server, client = get_components(pipe)
    cancel_done = asyncio.Event()
    stream_messages_sent_count = 0

    class Handler(BaseRequestHandler, DefaultPublisherSubscription):

        async def delayed_stream(self):
            nonlocal stream_messages_sent_count
            try:
                await asyncio.sleep(3)
                self._subscriber.on_next(Payload(b'success'))
                stream_messages_sent_count += 1
            except asyncio.CancelledError:
                cancel_done.set()

        def cancel(self):
            self._task.cancel()

        def request(self, n: int):
            self._task = asyncio.create_task(self.delayed_stream())

        async def request_stream(self, payload: Payload) -> Publisher:
            return self

    server.set_handler_using_factory(Handler)

    rx_client = RxRSocket(client)

    with pytest.raises(Exception):
        await asyncio.wait_for(rx_client.request_stream(
            Payload(b'request text')
        ).pipe(
            operators.to_list()
        ), 2)

    await cancel_done.wait()

    assert stream_messages_sent_count == 0


async def test_rx_support_request_response_cancel_on_timeout(pipe: Tuple[RSocketServer, RSocketClient]):
    server, client = get_components(pipe)
    response_sent = False

    class Handler(BaseRequestHandler):

        async def request_response(self, payload: Payload) -> Future:
            nonlocal response_sent
            await asyncio.sleep(3)
            response_sent = True
            return create_future(Payload(b'response'))

    server.set_handler_using_factory(Handler)

    rx_client = RxRSocket(client)

    with pytest.raises(Exception):
        await asyncio.wait_for(rx_client.request_response(
            Payload(b'request text')
        ), 2)

    assert not response_sent
