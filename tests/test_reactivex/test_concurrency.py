import asyncio
from datetime import datetime
from typing import Tuple, Optional, Awaitable

import reactivex
from reactivex import operators

from rsocket.frame_helpers import ensure_bytes
from rsocket.helpers import utf8_decode
from rsocket.payload import Payload
from rsocket.reactivex.reactivex_client import ReactiveXClient
from rsocket.reactivex.reactivex_handler import BaseReactivexHandler
from rsocket.reactivex.reactivex_handler_adapter import reactivex_handler_factory
from rsocket.rsocket_client import RSocketClient
from rsocket.rsocket_server import RSocketServer


class Handler(BaseReactivexHandler):

    def __init__(self, server_done: Optional[asyncio.Event] = None):
        self._server_done = server_done

    async def request_stream(self, payload: Payload):
        count = int(utf8_decode(payload.data))
        return reactivex.from_iterable((Payload(ensure_bytes('Feed Item: {}/{}'.format(index, count))) for index in range(count)))


async def measure_time(coroutine: Awaitable) -> float:
    start = datetime.now()
    await coroutine
    return (datetime.now() - start).total_seconds()


async def test_concurrent_streams(pipe: Tuple[RSocketServer, RSocketClient]):
    server, client = pipe

    server.set_handler_using_factory(reactivex_handler_factory(Handler))

    request_1 = asyncio.create_task(measure_time(ReactiveXClient(client).request_stream(Payload(b'2000')).pipe(
        operators.map(lambda payload: payload.data),
        operators.do_action(on_next=lambda x: print(x)),
        operators.to_list()
    )))

    request_2 = asyncio.create_task(measure_time(ReactiveXClient(client).request_stream(Payload(b'10')).pipe(
        operators.map(lambda payload: payload.data),
        operators.do_action(on_next=lambda x: print(x)),
        operators.to_list()
    )))

    results = await asyncio.gather(request_1, request_2)

    delta = abs(results[0] - results[1])

    assert delta > 0.8
