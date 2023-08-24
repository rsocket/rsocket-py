import asyncio
from typing import Tuple, Optional

import pytest

from rsocket.async_helpers import async_range
from rsocket.awaitable.awaitable_rsocket import AwaitableRSocket
from rsocket.frame_helpers import ensure_bytes
from rsocket.helpers import utf8_decode, create_response
from rsocket.payload import Payload
from rsocket.request_handler import BaseRequestHandler
from rsocket.rsocket_client import RSocketClient
from rsocket.rsocket_server import RSocketServer
from rsocket.streams.stream_from_async_generator import StreamFromAsyncGenerator
from tests.tools.helpers import measure_time


async def test_concurrent_streams(pipe: Tuple[RSocketServer, RSocketClient]):
    class Handler(BaseRequestHandler):

        def __init__(self, server_done: Optional[asyncio.Event] = None):
            self._server_done = server_done

        async def request_stream(self, payload: Payload):
            count = int(utf8_decode(payload.data))

            async def generator():
                async for index in async_range(count):
                    yield Payload(ensure_bytes('Feed Item: {}/{}'.format(index, count))), index == count - 1

            return StreamFromAsyncGenerator(generator)

    server, client = pipe

    server.set_handler_using_factory(Handler)

    request_1 = asyncio.create_task(measure_time(AwaitableRSocket(client).request_stream(Payload(b'2000'))))

    request_2 = asyncio.create_task(measure_time(AwaitableRSocket(client).request_stream(Payload(b'10'))))

    results = (await request_1, await request_2)

    print(results[0].delta, results[1].delta)
    delta = abs(results[0].delta - results[1].delta)

    assert len(results[0].result) == 2000
    assert len(results[1].result) == 10
    assert delta > 0.2


@pytest.mark.timeout(30)
@pytest.mark.parametrize('transport_id, expected_delta, expected_runtime', (
        ('tcp', 0.3, 3),
        ('aiohttp', 0.6, 7),
        ('quart', 1, 7),
        ('quic', 4, 13),
        ('http3', 5, 20),
))
async def test_concurrent_fragmented_responses(pipe_factory_by_id, unused_tcp_port, transport_id, expected_delta,
                                               expected_runtime):
    class Handler(BaseRequestHandler):
        async def request_response(self, request: Payload):
            data = 'a' * 100 * int(utf8_decode(request.data))
            return create_response(ensure_bytes(data))

    async with pipe_factory_by_id(transport_id)(unused_tcp_port,
                                                server_arguments={'handler_factory': Handler,
                                                                  'fragment_size_bytes': 100},
                                                client_arguments={'fragment_size_bytes': 100}) as (server, client):
        async def run():
            request_1 = asyncio.create_task(measure_time(client.request_response(Payload(b'10000'))))

            request_2 = asyncio.create_task(measure_time(client.request_response(Payload(b'10'))))
            return (await request_1, await request_2)

        measure_result = await measure_time(run())

        results = measure_result.result

        assert measure_result.delta < expected_runtime

        print(results[0].delta, results[1].delta)
        delta = abs(results[0].delta - results[1].delta)

        assert len(results[0].result.data) == 10000 * 100
        assert len(results[1].result.data) == 10 * 100
        assert delta > expected_delta

        await asyncio.sleep(1)
