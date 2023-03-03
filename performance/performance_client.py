import asyncio
import logging
import sys
from asyncio import Event
from typing import AsyncGenerator, Tuple

from reactivestreams.publisher import Publisher
from rsocket.awaitable.awaitable_rsocket import AwaitableRSocket
from rsocket.extensions.helpers import route, composite, authenticate_simple
from rsocket.extensions.mimetypes import WellKnownMimeTypes
from rsocket.fragment import Fragment
from rsocket.helpers import single_transport_provider
from rsocket.payload import Payload
from rsocket.rsocket_client import RSocketClient
from rsocket.streams.stream_from_async_generator import StreamFromAsyncGenerator
from rsocket.transports.tcp import TransportTCP
from tests.rsocket.helpers import to_json_bytes, create_large_random_data
from tests.tools.helpers import measure_time

data_size = 1920 # * 1080 * 3
large_data = create_large_random_data(data_size)


def sample_publisher(wait_for_requester_complete: Event,
                     response_count: int = 3,
                     data_generator=lambda index: ('Item to server from client on channel: %s' % index).encode('utf-8')
                     ) -> Publisher:
    async def generator() -> AsyncGenerator[Tuple[Fragment, bool], None]:
        for i in range(response_count):
            is_complete = (i + 1) == response_count

            message = data_generator(i)
            yield Payload(message), is_complete

            if is_complete:
                wait_for_requester_complete.set()
                break

    return StreamFromAsyncGenerator(generator)


class PerformanceClient:
    def __init__(self, server_port: int):
        self._server_port = server_port

    async def request_response(self):
        payload = Payload(b'The quick brown fox', composite(
            route('single_request'),
            authenticate_simple('user', '12345')
        ))

        return await self._client.request_response(payload)

    async def large_request(self):
        payload = Payload(large_data, composite(
            route('large'),
            authenticate_simple('user', '12345')
        ))

        return await self._client.request_response(payload)

    async def request_channel(self):
        payload = Payload(b'The quick brown fox', composite(
            route('channel'),
            authenticate_simple('user', '12345')
        ))
        publisher = sample_publisher(Event())

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

        connection = await asyncio.open_connection('localhost', self._server_port, limit=data_size + 3000)

        self._client = AwaitableRSocket(RSocketClient(
            single_transport_provider(TransportTCP(*connection, read_buffer_size=data_size + 3000)),
            metadata_encoding=WellKnownMimeTypes.MESSAGE_RSOCKET_COMPOSITE_METADATA)
        )

        await self._client.connect()
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        await self._client.__aexit__(exc_type, exc_val, exc_tb)


async def run_client():
    async with PerformanceClient(6565) as client:
        for i in range(10000):
            result = await measure_time(client.large_request())
            # print(result.delta)


if __name__ == '__main__':
    port = sys.argv[1] if len(sys.argv) > 1 else 6565
    logging.basicConfig(level=logging.ERROR)
    asyncio.run(run_client())
