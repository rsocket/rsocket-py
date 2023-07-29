import asyncio
import functools
from typing import Awaitable

import pytest

from rsocket.awaitable.awaitable_rsocket import AwaitableRSocket
from rsocket.helpers import create_future, create_response
from rsocket.payload import Payload
from rsocket.request_handler import BaseRequestHandler
from tests.rsocket.helpers import future_from_payload, get_components


@pytest.mark.timeout(4)
async def test_request_response_awaitable_wrapper(pipe):
    class Handler(BaseRequestHandler):
        async def request_response(self, request: Payload):
            return future_from_payload(request)

    server, client = get_components(pipe)
    server.set_handler_using_factory(Handler)

    response = await AwaitableRSocket(client).request_response(Payload(b'dog', b'cat'))
    assert response == Payload(b'data: dog', b'meta: cat')


async def test_request_response_repeated(pipe):
    class Handler(BaseRequestHandler):
        async def request_response(self, request: Payload):
            return future_from_payload(request)

    server, client = get_components(pipe)
    server.set_handler_using_factory(Handler)

    for x in range(2):
        response = await client.request_response(Payload(b'dog', b'cat'))
        assert response == Payload(b'data: dog', b'meta: cat')


async def test_request_response_failure(pipe):
    class Handler(BaseRequestHandler, asyncio.Future):
        async def request_response(self, payload: Payload):
            self.set_exception(RuntimeError(''))
            return self

    server, client = get_components(pipe)
    server.set_handler_using_factory(Handler)

    with pytest.raises(RuntimeError):
        await client.request_response(Payload())


async def test_request_response_cancellation(pipe):
    server_future = create_future()

    class Handler(BaseRequestHandler):
        async def request_response(self, payload: Payload):
            # return a future that will never complete.
            return server_future

    server, client = get_components(pipe)
    server.set_handler_using_factory(Handler)

    future = client.request_response(Payload())

    with pytest.raises(asyncio.TimeoutError):
        await asyncio.wait_for(asyncio.shield(server_future), 0.1)

    assert not server_future.cancelled()

    future.cancel()

    with pytest.raises(asyncio.CancelledError):
        await asyncio.wait_for(asyncio.shield(server_future), 0.1)

    with pytest.raises(asyncio.CancelledError):
        await future


async def test_request_response_bidirectional(pipe):
    class ServerHandler(BaseRequestHandler):
        @staticmethod
        def future_done(other: asyncio.Future, current: asyncio.Future):
            if current.cancelled():
                other.set_exception(RuntimeError('Canceled.'))
            elif current.exception():
                other.set_exception(current.exception())
            else:
                payload = current.result()
                payload.data = b'(server ' + payload.data + b')'
                payload.metadata = b'(server ' + payload.metadata + b')'
                other.set_result(payload)

        async def request_response(self, payload: Payload) -> Awaitable[Payload]:
            future = create_response()
            server.request_response(payload).add_done_callback(
                functools.partial(self.future_done, future))
            return future

    class ClientHandler(BaseRequestHandler):
        async def request_response(self, payload: Payload):
            return create_response(b'(client ' + payload.data + b')',
                                   b'(client ' + payload.metadata + b')')

    server, client = get_components(pipe)
    server.set_handler_using_factory(ServerHandler)
    client.set_handler_using_factory(ClientHandler)

    response = await client.request_response(Payload(b'data', b'metadata'))

    assert response.data == b'(server (client data))'
    assert response.metadata == b'(server (client metadata))'


@pytest.mark.parametrize('data_size_multiplier', (
        1,
        10
))
async def test_request_response_fragmented(lazy_pipe, data_size_multiplier):
    class Handler(BaseRequestHandler):
        async def request_response(self, request: Payload):
            return future_from_payload(request)

    async with lazy_pipe(
            server_arguments={'handler_factory': Handler, 'fragment_size_bytes': 64},
            client_arguments={'fragment_size_bytes': 64}) as (server, client):
        data = b'dog-dog-dog-dog-dog-dog-dog-dog-dog' * data_size_multiplier
        response = await client.request_response(Payload(data, b'cat'))

        assert response.data == b'data: ' + data


async def test_request_response_fragmented_empty_payload(lazy_pipe):
    class Handler(BaseRequestHandler):
        async def request_response(self, request: Payload):
            return create_response()

    async with lazy_pipe(
            server_arguments={'handler_factory': Handler, 'fragment_size_bytes': 64},
            client_arguments={'fragment_size_bytes': 64}) as (server, client):
        response = await client.request_response(Payload())

        assert response.data == b''
        assert response.metadata == b''
