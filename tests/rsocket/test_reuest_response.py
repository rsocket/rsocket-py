import asyncio
import functools

import pytest

from rsocket import BaseRequestHandler
from rsocket.payload import Payload


@pytest.mark.asyncio
async def test_request_response_repeated(pipe):
    class Handler(BaseRequestHandler):
        def request_response(self, request: Payload):
            future = asyncio.Future()
            future.set_result(Payload(b'data: ' + request.data,
                                      b'meta: ' + request.metadata))
            return future

    server, client = pipe
    server._handler = Handler(server)
    for x in range(2):
        response = await client.request_response(Payload(b'dog', b'cat'))
        assert response == Payload(b'data: dog', b'meta: cat')


@pytest.mark.asyncio
async def test_request_response_failure(pipe):
    class Handler(BaseRequestHandler, asyncio.Future):
        def request_response(self, payload: Payload):
            self.set_exception(RuntimeError(''))
            return self

    server, client = pipe
    server._handler = Handler(server)

    with pytest.raises(RuntimeError):
        await client.request_response(Payload(b''))


@pytest.mark.asyncio
async def test_request_response_cancellation(pipe):
    class Handler(BaseRequestHandler, asyncio.Future):
        def request_response(self, payload: Payload):
            # return a future that we'll never complete.
            return self

    server, client = pipe
    server._handler = handler = Handler(server)

    future = client.request_response(Payload(b''))
    asyncio.ensure_future(future)

    with pytest.raises(asyncio.TimeoutError):
        await asyncio.wait_for(asyncio.shield(handler), 0.1)
    assert not handler.cancelled()

    future.cancel()
    with pytest.raises(asyncio.CancelledError):
        await asyncio.wait_for(asyncio.shield(handler), 0.1)

    with pytest.raises(asyncio.CancelledError):
        await future


@pytest.mark.asyncio
async def test_request_response_bidirectional(pipe):
    def ready_future(data, metadata):
        future = asyncio.Future()
        future.set_result(Payload(data, metadata))
        return future

    class ServerHandler(BaseRequestHandler):
        @staticmethod
        def future_done(other: asyncio.Future, current: asyncio.Future):
            if current.cancelled():
                other.set_exception(RuntimeError("Canceled."))
            elif current.exception():
                other.set_exception(current.exception())
            else:
                payload = current.result()
                payload.data = b'(server ' + payload.data + b')'
                payload.metadata = b'(server ' + payload.metadata + b')'
                other.set_result(payload)

        def request_response(self, payload: Payload):
            future = asyncio.Future()
            self.socket.request_response(payload).add_done_callback(
                functools.partial(self.future_done, future))
            return future

    class ClientHandler(BaseRequestHandler):
        def request_response(self, payload: Payload):
            return ready_future(b'(client ' + payload.data + b')',
                                b'(client ' + payload.metadata + b')')

    server, client = pipe
    server._handler = ServerHandler(server)
    client._handler = ClientHandler(client)
    response = await client.request_response(Payload(b'data', b'metadata'))
    assert response.data == b'(server (client data))'
    assert response.metadata == b'(server (client metadata))'
