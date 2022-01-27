import asyncio
from typing import Optional

from rsocket.payload import Payload
from rsocket.request_handler import BaseRequestHandler
from rsocket.rsocket_client import RSocketClient
from rsocket.rsocket_server import RSocketServer


async def test_request_fire_and_forget(pipe):
    fire_and_forget_received = asyncio.Event()
    received_payload: Optional[Payload] = None

    class Handler(BaseRequestHandler):
        async def request_fire_and_forget(self, payload: Payload):
            nonlocal received_payload
            received_payload = payload
            fire_and_forget_received.set()

    server: RSocketServer = pipe[0]
    client: RSocketClient = pipe[1]
    server.set_handler_using_factory(Handler)

    client.fire_and_forget(Payload(b'dog', b'cat'))

    await fire_and_forget_received.wait()

    assert received_payload.data == b'dog'
    assert received_payload.metadata == b'cat'
