import asyncio
import logging
import sys
from asyncio import Event
from typing import AsyncGenerator, Tuple

from reactivestreams.subscriber import Subscriber
from reactivestreams.subscription import Subscription
from rsocket.extensions.helpers import route, composite, authenticate_simple
from rsocket.extensions.mimetypes import WellKnownMimeTypes
from rsocket.fragment import Fragment
from rsocket.helpers import single_transport_provider
from rsocket.payload import Payload
from rsocket.rsocket_client import RSocketClient
from rsocket.streams.stream_from_async_generator import StreamFromAsyncGenerator
from rsocket.transports.tcp import TransportTCP


def sample_publisher(wait_for_requester_complete: Event,
                     response_count: int = 3):
    async def generator() -> AsyncGenerator[Tuple[Fragment, bool], None]:
        current_response = 0
        for i in range(response_count):
            is_complete = (current_response + 1) == response_count

            message = 'Item to server from client on channel: %s' % current_response
            yield Fragment(message.encode('utf-8')), is_complete

            if is_complete:
                wait_for_requester_complete.set()
                break

            current_response += 1

    return StreamFromAsyncGenerator(generator)


class ChannelSubscriber(Subscriber):

    def __init__(self, wait_for_responder_complete: Event) -> None:
        super().__init__()
        self._wait_for_responder_complete = wait_for_responder_complete

    def on_subscribe(self, subscription: Subscription):
        self.subscription = subscription

    def on_next(self, value: Payload, is_complete=False):
        logging.info('From server on channel: ' + value.data.decode('utf-8'))
        if is_complete:
            self._wait_for_responder_complete.set()

    def on_error(self, exception: Exception):
        logging.error('Error from server on channel' + str(exception))
        self._wait_for_responder_complete.set()

    def on_complete(self):
        logging.info('Completed from server on channel')
        self._wait_for_responder_complete.set()


class StreamSubscriber(Subscriber):

    def __init__(self,
                 wait_for_complete: Event,
                 request_n_size=0):
        self._request_n_size = request_n_size
        self._wait_for_complete = wait_for_complete

    def on_next(self, value, is_complete=False):
        logging.info('RS: {}'.format(value))
        if is_complete:
            self._wait_for_complete.set()
        else:
            if self._request_n_size > 0:
                self.subscription.request(self._request_n_size)

    def on_complete(self):
        logging.info('RS: Complete')
        self._wait_for_complete.set()

    def on_error(self, exception):
        logging.info('RS: error: {}'.format(exception))
        self._wait_for_complete.set()

    def on_subscribe(self, subscription):
        # noinspection PyAttributeOutsideInit
        self.subscription = subscription


async def request_response(client: RSocketClient):
    payload = Payload(b'The quick brown fox', composite(
        route('single_request'),
        authenticate_simple('user', '12345')
    ))

    await client.request_response(payload)


async def request_channel(client: RSocketClient):
    channel_completion_event = Event()
    requester_completion_event = Event()
    payload = Payload(b'The quick brown fox', composite(
        route('channel'),
        authenticate_simple('user', '12345')
    ))
    publisher = sample_publisher(requester_completion_event)

    requested = client.request_channel(payload, publisher)

    requested.initial_request_n(5).subscribe(ChannelSubscriber(channel_completion_event))

    await channel_completion_event.wait()
    await requester_completion_event.wait()


async def request_stream_invalid_login(client: RSocketClient):
    payload = Payload(b'The quick brown fox', composite(
        route('stream'),
        authenticate_simple('user', 'wrong_password')
    ))
    completion_event = Event()
    client.request_stream(payload).initial_request_n(1).subscribe(StreamSubscriber(completion_event))
    await completion_event.wait()


async def request_stream(client: RSocketClient):
    payload = Payload(b'The quick brown fox', composite(
        route('stream'),
        authenticate_simple('user', '12345')
    ))
    completion_event = Event()
    client.request_stream(payload).subscribe(StreamSubscriber(completion_event))
    await completion_event.wait()


async def request_slow_stream(client: RSocketClient):
    payload = Payload(b'The quick brown fox', composite(
        route('slow_stream'),
        authenticate_simple('user', '12345')
    ))
    completion_event = Event()
    client.request_stream(payload).subscribe(StreamSubscriber(completion_event))
    await completion_event.wait()


async def request_fragmented_stream(client: RSocketClient):
    payload = Payload(b'The quick brown fox', composite(
        route('fragmented_stream'),
        authenticate_simple('user', '12345')
    ))
    completion_event = Event()
    client.request_stream(payload).subscribe(StreamSubscriber(completion_event))
    await completion_event.wait()


async def main(server_port):
    logging.info('Connecting to server at localhost:%s', server_port)

    connection = await asyncio.open_connection('localhost', server_port)

    async with RSocketClient(single_transport_provider(TransportTCP(*connection)),
                             metadata_encoding=WellKnownMimeTypes.MESSAGE_RSOCKET_COMPOSITE_METADATA) as client:
        await request_response(client)
        await request_stream(client)
        await request_slow_stream(client)
        await request_channel(client)
        await request_stream_invalid_login(client)
        await request_fragmented_stream(client)


if __name__ == '__main__':
    port = sys.argv[1] if len(sys.argv) > 1 else 6565
    logging.basicConfig(level=logging.DEBUG)
    asyncio.run(main(port))
