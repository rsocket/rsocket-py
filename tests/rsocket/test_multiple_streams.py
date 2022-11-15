from asyncio import Event
from typing import Tuple, Optional

from reactivestreams.publisher import Publisher
from reactivestreams.subscriber import DefaultSubscriber, Subscriber
from reactivestreams.subscription import Subscription
from rsocket.awaitable.awaitable_rsocket import AwaitableRSocket
from rsocket.awaitable.collector_subscriber import CollectorSubscriber
from rsocket.payload import Payload
from rsocket.request_handler import BaseRequestHandler
from rsocket.rsocket_client import RSocketClient
from rsocket.rsocket_server import RSocketServer
from rsocket.streams.stream_from_generator import StreamFromGenerator
from tests.rsocket.helpers import get_components


async def test_request_channel_and_stream_intertwined(pipe: Tuple[RSocketServer, RSocketClient]):
    server, client = get_components(pipe)
    messages_received_from_client_client = []
    message_count_sent_from_requester = 0
    maximum_message_count = 5
    take_only_n = 2

    def responder_generator():
        for x in range(3):
            yield Payload('Feed Item: {}'.format(x).encode('utf-8')), x == 2

    class Handler(BaseRequestHandler, DefaultSubscriber):

        def on_next(self, value: Payload, is_complete=False):
            messages_received_from_client_client.append(value)

            if len(messages_received_from_client_client) < take_only_n:
                self.subscription.request(1)
            else:
                self.subscription.cancel()

        def on_subscribe(self, subscription: Subscription):
            super().on_subscribe(subscription)
            subscription.request(1)

        async def request_stream(self, payload: Payload) -> Publisher:
            return StreamFromGenerator(responder_generator)

        async def request_channel(self, payload: Payload) -> Tuple[Optional[Publisher], Optional[Subscriber]]:
            return StreamFromGenerator(responder_generator), self

    server.set_handler_using_factory(Handler)

    def requester_generator():
        nonlocal message_count_sent_from_requester
        for x in range(maximum_message_count):
            message_count_sent_from_requester += 1
            yield Payload('Feed Item: {}'.format(x).encode('utf-8')), x == (maximum_message_count - 1)

    sending_done = Event()

    client.request_channel(
        Payload(b'request text channel'),
        publisher=StreamFromGenerator(requester_generator),
        sending_done=sending_done
    ).subscribe(CollectorSubscriber(limit_count=1))

    messages_received_from_server_stream = await AwaitableRSocket(client).request_stream(Payload(b'request text stream'))

    await sending_done.wait()

    maximum_message_received_by_responder = min(maximum_message_count, take_only_n)

    assert message_count_sent_from_requester == maximum_message_received_by_responder

    assert len(messages_received_from_client_client) == maximum_message_received_by_responder

    assert len(messages_received_from_server_stream) == 3

    for i in range(maximum_message_received_by_responder):
        assert messages_received_from_client_client[i].data == ('Feed Item: %d' % i).encode()
