import asyncio
import logging
from asyncio import Event
from typing import AsyncGenerator, Tuple

from reactivestreams.subscriber import Subscriber
from reactivestreams.subscription import Subscription
from rsocket.extensions.mimetypes import WellKnownMimeTypes
from rsocket.fragment import Fragment
from rsocket.payload import Payload
from rsocket.streams.stream_from_generator import StreamFromGenerator
from rsocket.routing.helpers import route, composite, authenticate_simple
from rsocket.rsocket_client import RSocketClient


class RequestChannel(StreamFromGenerator, Subscriber):
    def __init__(self,
                 wait_for_responder_complete: Event,
                 wait_for_requester_complete: Event,
                 response_count: int = 3):
        super().__init__()
        self._wait_for_responder_complete = wait_for_responder_complete
        self._wait_for_requester_complete = wait_for_requester_complete
        self._response_count = response_count
        self._current_response = 0

    async def generate_next_n(self, n: int) -> AsyncGenerator[Tuple[Fragment, bool], None]:
        for i in range(n):
            is_complete = (self._current_response + 1) == self._response_count

            message = 'Item to server from client on channel: %s' % self._current_response
            yield Fragment(message.encode('utf-8'), b''), is_complete

            self.subscription.request(1)
            if is_complete:
                self._wait_for_requester_complete.set()
                break

            self._current_response += 1

    def on_subscribe(self, subscription: Subscription):
        self.subscription = subscription

    def on_next(self, value: Payload, is_complete=False):
        logging.info('From server on channel: ' + value.data.decode('utf-8'))

    def on_error(self, exception: Exception):
        logging.error('Error from server on channel' + str(exception))
        self._wait_for_responder_complete.set()

    def on_complete(self):
        logging.info('Completed from server on channel')
        self._wait_for_responder_complete.set()


class StreamSubscriber(Subscriber):

    def __init__(self, wait_for_complete: Event):
        self._wait_for_complete = wait_for_complete

    def on_next(self, value, is_complete=False):
        logging.info('RS: {}'.format(value))
        self.subscription.request(1)

    def on_complete(self):
        logging.info('RS: Complete')
        self._wait_for_complete.set()

    def on_error(self, exception):
        logging.info('RS: error: {}'.format(exception))
        self._wait_for_complete.set()

    def on_subscribe(self, subscription):
        # noinspection PyAttributeOutsideInit
        self.subscription = subscription


async def communicate(reader, writer):
    async with RSocketClient(reader, writer,
                             metadata_encoding=WellKnownMimeTypes.MESSAGE_RSOCKET_COMPOSITE_METADATA) as socket:
        await request_response(socket)
        await request_stream(socket)
        await request_slow_stream(socket)
        await request_channel(socket)
        await request_stream_invalid_login(socket)
        await request_fragmented_stream(socket)


async def request_response(socket: RSocketClient):
    payload = Payload(b'The quick brown fox',
                      composite(route('single_request'),
                                authenticate_simple('user', '12345')))

    await socket.request_response(payload)


async def request_channel(socket: RSocketClient):
    channel_completion_event = Event()
    requester_completion_event = Event()
    channel_payload = Payload(b'The quick brown fox',
                              composite(route('channel'),
                                        authenticate_simple('user', '12345')))
    channel = RequestChannel(channel_completion_event, requester_completion_event)
    requested = socket.request_channel(channel_payload, channel)
    requested.limit_rate(1).subscribe(channel)

    await channel_completion_event.wait()
    await requester_completion_event.wait()


async def request_stream_invalid_login(socket: RSocketClient):
    payload = Payload(b'The quick brown fox', composite(route('stream'),
                                                        authenticate_simple('user', 'wrong_password')))
    completion_event = Event()
    socket.request_stream(payload).limit_rate(1).subscribe(StreamSubscriber(completion_event))
    await completion_event.wait()


async def request_stream(socket: RSocketClient):
    payload = Payload(b'The quick brown fox', composite(route('stream'),
                                                        authenticate_simple('user', '12345')))
    completion_event = Event()
    socket.request_stream(payload).subscribe(StreamSubscriber(completion_event))
    await completion_event.wait()


async def request_slow_stream(socket: RSocketClient):
    payload = Payload(b'The quick brown fox', composite(route('slow_stream'),
                                                        authenticate_simple('user', '12345')))
    completion_event = Event()
    socket.request_stream(payload).subscribe(StreamSubscriber(completion_event))
    await completion_event.wait()


async def request_fragmented_stream(socket: RSocketClient):
    payload = Payload(b'The quick brown fox', composite(route('fragmented_stream'),
                                                        authenticate_simple('user', '12345')))
    completion_event = Event()
    socket.request_stream(payload).subscribe(StreamSubscriber(completion_event))
    await completion_event.wait()


if __name__ == '__main__':
    logging.basicConfig(level=logging.DEBUG)
    loop = asyncio.get_event_loop()
    try:
        connection = loop.run_until_complete(asyncio.open_connection(
            'localhost', 6565))
        loop.run_until_complete(communicate(*connection))
    finally:
        loop.close()
