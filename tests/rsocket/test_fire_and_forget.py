import asyncio
from typing import Optional

from rsocket.payload import Payload
from rsocket.request_handler import BaseRequestHandler


async def test_request_fire_and_forget(fail_on_error_log, lazy_pipe):
    fire_and_forget_received = asyncio.Event()
    received_payload: Optional[Payload] = None

    class Handler(BaseRequestHandler):
        async def request_fire_and_forget(self, payload: Payload):
            nonlocal received_payload
            received_payload = payload
            fire_and_forget_received.set()

    async with lazy_pipe(
            server_arguments={'handler_factory': Handler}) as (server, client):
        client.fire_and_forget(Payload(b'dog', b'cat'))

        await fire_and_forget_received.wait()

        assert received_payload.data == b'dog'
        assert received_payload.metadata == b'cat'
