import asyncio
from datetime import datetime
from typing import Tuple, Optional, Awaitable

from rsocket.async_helpers import async_range
from rsocket.awaitable.awaitable_rsocket import AwaitableRSocket
from rsocket.frame_helpers import ensure_bytes
from rsocket.helpers import utf8_decode
from rsocket.payload import Payload
from rsocket.request_handler import BaseRequestHandler
from rsocket.rsocket_client import RSocketClient
from rsocket.rsocket_server import RSocketServer
from rsocket.streams.stream_from_async_generator import StreamFromAsyncGenerator


class Handler(BaseRequestHandler):

    def __init__(self, server_done: Optional[asyncio.Event] = None):
        self._server_done = server_done

    async def request_stream(self, payload: Payload):
        count = int(utf8_decode(payload.data))

        async def generator():
            async for index in async_range(count):
                yield Payload(ensure_bytes('Feed Item: {}/{}'.format(index, count))), index == count - 1

        return StreamFromAsyncGenerator(generator)


async def measure_time(coroutine: Awaitable) -> float:
    start = datetime.now()
    await coroutine
    return (datetime.now() - start).total_seconds()


async def test_concurrent_streams(pipe: Tuple[RSocketServer, RSocketClient]):
    server, client = pipe

    server.set_handler_using_factory(Handler)

    request_1 = asyncio.create_task(measure_time(AwaitableRSocket(client).request_stream(Payload(b'2000'))))

    request_2 = asyncio.create_task(measure_time(AwaitableRSocket(client).request_stream(Payload(b'10'))))

    results = (await request_1, await request_2)

    print(results)
    delta = abs(results[0] - results[1])

    assert delta > 0.8
