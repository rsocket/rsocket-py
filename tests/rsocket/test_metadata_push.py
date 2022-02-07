import asyncio
from typing import Optional

from rsocket.payload import Payload
from rsocket.request_handler import BaseRequestHandler
from rsocket.rsocket_client import RSocketClient
from rsocket.rsocket_server import RSocketServer


async def test_metadata_push(pipe):
    metadata_push_received = asyncio.Event()
    received_payload: Optional[Payload] = None

    class Handler(BaseRequestHandler):
        async def on_metadata_push(self, payload: Payload):
            nonlocal received_payload
            received_payload = payload
            metadata_push_received.set()

    server: RSocketServer = pipe[0]
    client: RSocketClient = pipe[1]
    server.set_handler_using_factory(Handler)

    client.metadata_push(b'cat')

    await metadata_push_received.wait()

    assert received_payload.data is None
    assert received_payload.metadata == b'cat'
