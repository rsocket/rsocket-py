from asyncio import Event
from typing import Optional

import pytest

from rsocket.helpers import create_future
from rsocket.local_typing import Awaitable
from rsocket.payload import Payload
from rsocket.request_handler import BaseRequestHandler


@pytest.mark.parametrize('data_mimetype', (
        'application/json',
        'custom_defined/custom_type'
))
async def test_setup_with_explicit_data_encoding(lazy_pipe, data_mimetype):
    received_data_encoding_event = Event()
    received_data_encoding: Optional[bytes] = None

    class ServerHandler(BaseRequestHandler):
        def __init__(self, socket):
            super().__init__(socket)
            self._authenticated = False

        async def on_setup(self,
                           data_encoding: bytes,
                           metadata_encoding: bytes,
                           payload: Payload):
            nonlocal received_data_encoding
            received_data_encoding = data_encoding
            received_data_encoding_event.set()

        async def request_response(self, payload: Payload) -> Awaitable[Payload]:
            return create_future(Payload(b'response'))

    async with lazy_pipe(
            client_arguments={
                'data_encoding': data_mimetype
            },
            server_arguments={
                'handler_factory': ServerHandler
            }):
        await received_data_encoding_event.wait()

        assert received_data_encoding == data_mimetype.encode()
