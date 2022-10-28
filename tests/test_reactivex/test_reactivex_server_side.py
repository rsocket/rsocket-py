from typing import Tuple

import reactivex
from reactivex import operators, Observable

from rsocket.frame_helpers import ensure_bytes
from rsocket.payload import Payload
from rsocket.reactivex.reactivex_client import ReactiveXClient
from rsocket.reactivex.reactivex_handler import BaseReactivexHandler
from rsocket.reactivex.reactivex_handler_adapter import reactivex_handler_factory
from rsocket.rsocket_client import RSocketClient
from rsocket.rsocket_server import RSocketServer


async def test_serve_reactivex_stream(pipe: Tuple[RSocketServer, RSocketClient]):
    server, client = pipe

    class Handler(BaseReactivexHandler):
        async def request_stream(self, payload: Payload) -> Observable:
            return reactivex.from_iterable((Payload(ensure_bytes('Feed Item: {}'.format(index))) for index in range(3)))

    server.set_handler_using_factory(reactivex_handler_factory(Handler))

    received_messages = await ReactiveXClient(client).request_stream(Payload(b'request text'),
                                                                     request_limit=2).pipe(
        operators.map(lambda payload: payload.data),
        operators.to_list()
    )

    assert len(received_messages) == 3
    assert received_messages[0] == b'Feed Item: 0'
    assert received_messages[1] == b'Feed Item: 1'
    assert received_messages[2] == b'Feed Item: 2'


async def test_serve_reactivex_response(pipe: Tuple[RSocketServer, RSocketClient]):
    server, client = pipe

    class Handler(BaseReactivexHandler):
        async def request_response(self, payload: Payload) -> Observable:
            return reactivex.of(Payload(b'response value'))

    server.set_handler_using_factory(reactivex_handler_factory(Handler))

    received_messages = await ReactiveXClient(client).request_response(Payload(b'request text')).pipe(
        operators.map(lambda payload: payload.data),
        operators.to_list()
    )

    assert len(received_messages) == 1
    assert received_messages[0] == b'response value'
