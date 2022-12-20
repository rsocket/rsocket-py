import asyncio
from time import sleep
from typing import Tuple

import pytest
import reactivex
from reactivex import operators, Observable
from reactivex.operators._tofuture import to_future_
from reactivex.scheduler import ThreadPoolScheduler
from reactivex.scheduler.eventloop import AsyncIOScheduler

from rsocket.exceptions import RSocketProtocolError
from rsocket.frame_helpers import ensure_bytes
from rsocket.payload import Payload
from rsocket.reactivex.reactivex_client import ReactiveXClient
from rsocket.reactivex.reactivex_handler import BaseReactivexHandler
from rsocket.reactivex.reactivex_handler_adapter import reactivex_handler_factory
from rsocket.rsocket_client import RSocketClient
from rsocket.rsocket_server import RSocketServer


async def test_serve_reactivex_stream_disconnected(pipe: Tuple[RSocketServer, RSocketClient]):
    sent_counter = 0

    def increment_sent_counter():
        nonlocal sent_counter
        sent_counter += 1

    def delayed(message):
        sleep(0.3)
        return message

    class Handler(BaseReactivexHandler):

        async def request_stream(self, payload: Payload) -> Observable:
            return reactivex.from_((delayed('Feed Item: {}'.format(index)) for index in range(10)),
                                   ThreadPoolScheduler()).pipe(
                operators.do_action(on_next=lambda _: increment_sent_counter()),
                operators.map(lambda _: Payload(ensure_bytes(_)))
            )

    server, client = pipe

    server.set_handler_using_factory(reactivex_handler_factory(Handler))

    async def request():
        await ReactiveXClient(client).request_stream(Payload(b'request text'),
                                                     request_limit=2).pipe(
            operators.map(lambda payload: payload.data),
            operators.to_list(),
            to_future_(scheduler=AsyncIOScheduler(loop=asyncio.get_event_loop()))
        )

    task = asyncio.create_task(request())

    await asyncio.sleep(1)

    await client.close()

    assert 0 < sent_counter < 5

    with pytest.raises(RSocketProtocolError):
        await task
