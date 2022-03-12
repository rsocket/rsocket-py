import asyncio
from typing import Optional

from rsocket.awaitable.awaitable_rsocket import AwaitableRSocket
from rsocket.payload import Payload
from rsocket.request_handler import BaseRequestHandler
from rsocket.rsocket_client import RSocketClient
from rsocket.rsocket_server import RSocketServer


class MetadataPushHandler(BaseRequestHandler):
    def __init__(self, socket):
        super().__init__(socket)
        self.received = asyncio.Event()
        self.received_payload: Optional[Payload] = None

    async def on_metadata_push(self, payload: Payload):
        self.received_payload = payload
        self.received.set()


async def test_metadata_push(pipe):
    handler: Optional[MetadataPushHandler] = None

    def handler_factory(socket):
        nonlocal handler
        handler = MetadataPushHandler(socket)
        return handler

    server: RSocketServer = pipe[0]
    client: RSocketClient = pipe[1]
    server.set_handler_using_factory(handler_factory)

    client.metadata_push(b'cat')

    await handler.received.wait()

    assert handler.received_payload.data is None
    assert handler.received_payload.metadata == b'cat'


async def test_metadata_push_awaitable_client(pipe):
    handler: Optional[MetadataPushHandler] = None

    def handler_factory(socket):
        nonlocal handler
        handler = MetadataPushHandler(socket)
        return handler

    server: RSocketServer = pipe[0]
    client = AwaitableRSocket(pipe[1])
    server.set_handler_using_factory(handler_factory)

    client.metadata_push(b'cat')

    await handler.received.wait()

    assert handler.received_payload.data is None
    assert handler.received_payload.metadata == b'cat'
