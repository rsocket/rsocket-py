import asyncio
from asyncio import Future, Event
from typing import Optional

from rsocket.error_codes import ErrorCode
from rsocket.exceptions import RSocketRejected, RSocketApplicationError
from rsocket.extensions.authentication import AuthenticationSimple
from rsocket.extensions.authentication_types import WellKnownAuthenticationTypes
from rsocket.extensions.composite_metadata import CompositeMetadata
from rsocket.extensions.mimetypes import WellKnownMimeTypes
from rsocket.frame import CONNECTION_STREAM_ID
from rsocket.payload import Payload
from rsocket.request_handler import BaseRequestHandler
from rsocket.routing.helpers import composite, authenticate_simple
from tests.rsocket.helpers import bits, data_bits, build_frame


async def test_authentication_frame_bearer():
    data = build_frame(
        bits(1, 1, 'Well known metadata type'),
        bits(7, WellKnownMimeTypes.MESSAGE_RSOCKET_AUTHENTICATION.value.id, 'Mime ID'),
        bits(24, 9, 'Metadata length'),
        bits(1, 1, 'Well known authentication type'),
        bits(7, WellKnownAuthenticationTypes.BEARER.value.id, 'Authentication ID'),
        data_bits(b'abcd1234')
    )

    composite_metadata = CompositeMetadata()
    composite_metadata.parse(data)

    auth = composite_metadata.items[0].authentication
    assert auth.token == b'abcd1234'

    serialized_data = composite_metadata.serialize()

    assert serialized_data == data


async def test_authentication_frame_simple():
    data = build_frame(
        bits(1, 1, 'Well known metadata type'),
        bits(7, WellKnownMimeTypes.MESSAGE_RSOCKET_AUTHENTICATION.value.id, 'Mime ID'),
        bits(24, 19, 'Metadata length'),
        bits(1, 1, 'Well known authentication type'),
        bits(7, WellKnownAuthenticationTypes.SIMPLE.value.id, 'Authentication ID'),
        bits(16, 8, 'Username length'),
        data_bits(b'username'),
        data_bits(b'password')
    )

    composite_metadata = CompositeMetadata()
    composite_metadata.parse(data)

    auth = composite_metadata.items[0].authentication
    assert auth.username == b'username'
    assert auth.password == b'password'

    serialized_data = composite_metadata.serialize()

    assert serialized_data == data


async def test_authentication_success_on_setup(lazy_pipe):
    class Handler(BaseRequestHandler):
        def __init__(self, socket):
            super().__init__(socket)
            self._authenticated = False

        async def on_setup(self,
                           data_encoding: bytes,
                           metadata_encoding: bytes,
                           payload: Payload):
            composite_metadata = self._parse_composite_metadata(payload.metadata)
            authentication: AuthenticationSimple = composite_metadata.items[0].authentication
            if authentication.username != b'user' or authentication.password != b'12345':
                raise RSocketRejected(CONNECTION_STREAM_ID)

            self._authenticated = True

        async def request_response(self, payload: Payload) -> Future:
            if not self._authenticated:
                raise RSocketApplicationError("Not authenticated")

            future = asyncio.get_event_loop().create_future()
            future.set_result(Payload(b'response'))
            return future

    async with lazy_pipe(
            client_arguments={'setup_payload': Payload(metadata=composite(authenticate_simple('user', '12345')))},
            server_arguments={'handler_factory': Handler}) as (server, client):
        result = await client.request_response(Payload(b'request'))

        assert result.data == b'response'


async def test_authentication_failure_on_setup(lazy_pipe):
    received_error_event = Event()
    received_error: Optional[tuple] = None

    class ServerHandler(BaseRequestHandler):
        def __init__(self, socket):
            super().__init__(socket)
            self._authenticated = False

        async def on_setup(self,
                           data_encoding: bytes,
                           metadata_encoding: bytes,
                           payload: Payload):
            composite_metadata = self._parse_composite_metadata(payload.metadata)
            authentication: AuthenticationSimple = composite_metadata.items[0].authentication
            if authentication.username != b'user' or authentication.password != b'12345':
                raise RSocketApplicationError('Authentication error')

            self._authenticated = True

    class ClientHandler(BaseRequestHandler):
        async def on_error(self, error_code: ErrorCode, payload: Payload):
            nonlocal received_error
            received_error = (error_code, payload)
            received_error_event.set()

    async with lazy_pipe(
            client_arguments={
                'handler_factory': ClientHandler,
                'setup_payload': Payload(metadata=composite(authenticate_simple('user', 'wrong_password')))
            },
            server_arguments={
                'handler_factory': ServerHandler
            }) as (server, client):
        await received_error_event.wait()

        assert received_error[0] == ErrorCode.APPLICATION_ERROR
        assert received_error[1] == Payload(b'Authentication error', b'')
