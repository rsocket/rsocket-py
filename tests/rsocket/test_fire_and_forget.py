import asyncio
from typing import Optional

from rsocket.awaitable.awaitable_rsocket import AwaitableRSocket
from rsocket.payload import Payload
from rsocket.request_handler import BaseRequestHandler


class FireAndForgetHandler(BaseRequestHandler):
    def __init__(self, socket):
        super().__init__(socket)
        self.received = asyncio.Event()
        self.received_payload: Optional[Payload] = None

    async def request_fire_and_forget(self, payload: Payload):
        self.received_payload = payload
        self.received.set()


async def test_request_fire_and_forget(lazy_pipe):
    handler: Optional[FireAndForgetHandler] = None

    def handler_factory(socket):
        nonlocal handler
        handler = FireAndForgetHandler(socket)
        return handler

    async with lazy_pipe(
            server_arguments={'handler_factory': handler_factory}) as (server, client):
        client.fire_and_forget(Payload(b'dog', b'cat'))

        await handler.received.wait()

        assert handler.received_payload.data == b'dog'
        assert handler.received_payload.metadata == b'cat'


async def test_request_fire_and_forget_awaitable_client(lazy_pipe):
    handler: Optional[FireAndForgetHandler] = None

    def handler_factory(socket):
        nonlocal handler
        handler = FireAndForgetHandler(socket)
        return handler

    async with lazy_pipe(
            server_arguments={'handler_factory': handler_factory}) as (server, client):
        AwaitableRSocket(client).fire_and_forget(Payload(b'dog', b'cat'))

        await handler.received.wait()

        assert handler.received_payload.data == b'dog'
        assert handler.received_payload.metadata == b'cat'
