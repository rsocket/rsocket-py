import asyncio
import logging
from asyncio import Event
from typing import AsyncGenerator, Tuple

from reactivestreams.publisher import Publisher
from reactivestreams.subscriber import Subscriber
from reactivestreams.subscription import Subscription
from rsocket.awaitable.awaitable_rsocket import AwaitableRSocket
from rsocket.extensions.helpers import route, composite, authenticate_simple
from rsocket.extensions.mimetypes import WellKnownMimeTypes
from rsocket.fragment import Fragment
from rsocket.helpers import single_transport_provider
from rsocket.payload import Payload
from rsocket.rsocket_client import RSocketClient
from rsocket.streams.stream_from_async_generator import StreamFromAsyncGenerator
from rsocket.transports.tcp import TransportTCP
from tests.rsocket.helpers import to_json_bytes


def sample_publisher(wait_for_requester_complete: Event,
                     response_count: int = 3) -> Publisher:
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
        logging.info(f'Performance Client: value_length {len(value.data)}')
        if is_complete:
            self._wait_for_complete.set()
        else:
            if self._request_n_size > 0:
                self.subscription.request(self._request_n_size)

    def on_complete(self):
        logging.info('Performance Client: Complete')
        self._wait_for_complete.set()

    def on_error(self, exception):
        logging.error('Performance Client: {}'.format(exception))
        self._wait_for_complete.set()

    def on_subscribe(self, subscription):
        # noinspection PyAttributeOutsideInit
        self.subscription = subscription


class PerformanceClient:
    def __init__(self, server_port: int):
        self._server_port = server_port

    async def request_response(self):
        payload = Payload(b'The quick brown fox', composite(
            route('single_request'),
            authenticate_simple('user', '12345')
        ))

        return await self._client.request_response(payload)

    async def request_channel(self):
        requester_completion_event = Event()
        payload = Payload(b'The quick brown fox', composite(
            route('channel'),
            authenticate_simple('user', '12345')
        ))
        publisher = sample_publisher(requester_completion_event)

        return await self._client.request_channel(payload, publisher, limit_rate=5)

    async def request_stream_invalid_login(self):
        payload = Payload(b'The quick brown fox', composite(
            route('stream'),
            authenticate_simple('user', 'wrong_password')
        ))

        return await self._client.request_stream(payload, 1)

    async def request_stream(self, response_count: int = 3, response_size=100):
        payload = Payload(to_json_bytes({'response_count': response_count,
                                         'response_size': response_size}),
                          composite(
                              route('stream'),
                              authenticate_simple('user', '12345')
                          ))

        return await self._client.request_stream(payload)

    async def request_slow_stream(self):
        payload = Payload(b'The quick brown fox', composite(
            route('slow_stream'),
            authenticate_simple('user', '12345')
        ))

        return await self._client.request_stream(payload)

    async def request_fragmented_stream(self):
        payload = Payload(b'The quick brown fox', composite(
            route('fragmented_stream'),
            authenticate_simple('user', '12345')
        ))

        return await self._client.request_stream(payload)

    async def __aenter__(self):
        logging.info('Connecting to server at localhost:%s', self._server_port)

        connection = await asyncio.open_connection('localhost', self._server_port)

        self._client = AwaitableRSocket(RSocketClient(
            single_transport_provider(TransportTCP(*connection)),
            metadata_encoding=WellKnownMimeTypes.MESSAGE_RSOCKET_COMPOSITE_METADATA)
        )

        await self._client.connect()
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        await self._client.__aexit__(exc_type, exc_val, exc_tb)
