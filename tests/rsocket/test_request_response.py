import asyncio
import functools

import pytest

from rsocket.awaitable.awaitable_rsocket import AwaitableRSocket
from rsocket.helpers import create_future
from rsocket.payload import Payload
from rsocket.request_handler import BaseRequestHandler
from tests.rsocket.helpers import future_from_payload


@pytest.mark.timeout(4)
async def test_request_response_awaitable_wrapper(pipe):
    class Handler(BaseRequestHandler):
        async def request_response(self, request: Payload):
            return future_from_payload(request)

    server, client = pipe
    server._handler = Handler(server)

    response = await AwaitableRSocket(client).request_response(Payload(b'dog', b'cat'))
    assert response == Payload(b'data: dog', b'meta: cat')


async def test_request_response_repeated(pipe):
    class Handler(BaseRequestHandler):
        async def request_response(self, request: Payload):
            return future_from_payload(request)

    server, client = pipe
    server._handler = Handler(server)

    for x in range(2):
        response = await client.request_response(Payload(b'dog', b'cat'))
        assert response == Payload(b'data: dog', b'meta: cat')


async def test_request_response_failure(pipe):
    class Handler(BaseRequestHandler, asyncio.Future):
        async def request_response(self, payload: Payload):
            self.set_exception(RuntimeError(''))
            return self

    server, client = pipe
    server._handler = Handler(server)

    with pytest.raises(RuntimeError):
        await client.request_response(Payload())


async def test_request_response_cancellation(pipe):
    server_future = create_future()

    class Handler(BaseRequestHandler):
        async def request_response(self, payload: Payload):
            # return a future that will never complete.
            return server_future

    server, client = pipe
    server._handler = Handler(server)

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

        async def request_response(self, payload: Payload):
            future = create_future()
            self.socket.request_response(payload).add_done_callback(
                functools.partial(self.future_done, future))
            return future

    class ClientHandler(BaseRequestHandler):
        async def request_response(self, payload: Payload):
            return create_future(Payload(b'(client ' + payload.data + b')',
                                         b'(client ' + payload.metadata + b')'))

    server, client = pipe
    server._handler = ServerHandler(server)
    client._handler = ClientHandler(client)

    response = await client.request_response(Payload(b'data', b'metadata'))

    assert response.data == b'(server (client data))'
    assert response.metadata == b'(server (client metadata))'
