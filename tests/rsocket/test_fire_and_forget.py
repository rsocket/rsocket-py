import asyncio
from typing import Optional

import pytest

from rsocket.awaitable.awaitable_rsocket import AwaitableRSocket
from rsocket.payload import Payload
from rsocket.request_handler import BaseRequestHandler


class FireAndForgetHandler(BaseRequestHandler):
    def __init__(self):
        self.received = asyncio.Event()
        self.received_payload: Optional[Payload] = None

    async def request_fire_and_forget(self, payload: Payload):
        self.received_payload = payload
        self.received.set()


async def test_request_fire_and_forget(lazy_pipe):
    handler: Optional[FireAndForgetHandler] = None

    def handler_factory():
        nonlocal handler
        handler = FireAndForgetHandler()
        return handler

    async with lazy_pipe(
            server_arguments={'handler_factory': handler_factory}) as (server, client):
        await client.fire_and_forget(Payload(b'dog', b'cat'))

        await handler.received.wait()

        assert handler.received_payload.data == b'dog'
        assert handler.received_payload.metadata == b'cat'

    await asyncio.sleep(2)  # wait for server to close


@pytest.mark.timeout(15)
async def test_request_fire_and_forget_fragmented(lazy_pipe):
    handler: Optional[FireAndForgetHandler] = None

    def handler_factory():
        nonlocal handler
        handler = FireAndForgetHandler()
        return handler

    async with lazy_pipe(
            server_arguments={'handler_factory': handler_factory,
                              'fragment_size_bytes': 10240},
            client_arguments={'fragment_size_bytes': 10240}) as (server, client):
        data = bytearray(16777210)

        await client.fire_and_forget(Payload(data))

        await handler.received.wait()

        assert handler.received_payload.data == data
        assert handler.received_payload.metadata == b''

    await asyncio.sleep(2)  # wait for server to close


async def test_request_fire_and_forget_awaitable_client(lazy_pipe):
    handler: Optional[FireAndForgetHandler] = None

    def handler_factory():
        nonlocal handler
        handler = FireAndForgetHandler()
        return handler

    async with lazy_pipe(
            server_arguments={'handler_factory': handler_factory}) as (server, client):
        await AwaitableRSocket(client).fire_and_forget(Payload(b'dog', b'cat'))

        await handler.received.wait()

        assert handler.received_payload.data == b'dog'
        assert handler.received_payload.metadata == b'cat'
