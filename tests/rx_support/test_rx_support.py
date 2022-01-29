import asyncio
import logging
from typing import Tuple

from rx import operators

from reactivestreams.publisher import Publisher
from reactivestreams.subscription import DefaultSubscription
from rsocket.payload import Payload
from rsocket.request_handler import BaseRequestHandler
from rsocket.rsocket_client import RSocketClient
from rsocket.rsocket_server import RSocketServer
from rsocket.rx_support.rx_rsocket_client import RxRSocketClient


async def test_request_stream_properly_finished(pipe: Tuple[RSocketServer, RSocketClient]):
    server, client = pipe

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
                    logging.debug('Sending payload %s', value)
                    subscriber.on_next(value)
                loop.call_soon(subscriber.on_complete)
            except asyncio.CancelledError:
                pass

    server.set_handler_using_factory(Handler)

    rx_client = RxRSocketClient(client)
    received_messages = await rx_client.request_stream(Payload(b'')).pipe(
        operators.map(lambda payload: payload.data),
        operators.to_list()
    )

    assert len(received_messages) == 4
    assert received_messages[0] == b'Feed Item: 0'
    assert received_messages[1] == b'Feed Item: 1'
    assert received_messages[2] == b'Feed Item: 2'
    assert received_messages[3] == b''
