import asyncio
from typing import Tuple

import pytest

from rsocket.error_codes import ErrorCode
from rsocket.extensions.mimetypes import WellKnownMimeTypes
from rsocket.frame import SetupFrame, ResumeFrame
from rsocket.payload import Payload
from rsocket.request_handler import BaseRequestHandler
from rsocket.rsocket_client import RSocketClient
from rsocket.rsocket_server import RSocketServer
from tests.rsocket.misbehaving_rsocket import MisbehavingRSocket


@pytest.mark.allow_error_log(regex_filter='Protocol error')
async def test_setup_resume_unsupported(pipe_tcp_without_auto_connect: Tuple[RSocketServer, RSocketClient]):
    _, client = pipe_tcp_without_auto_connect
    received_error_code = None
    error_received = asyncio.Event()

    class Handler(BaseRequestHandler):
        async def on_error(self, error_code: ErrorCode, payload: Payload):
            nonlocal received_error_code
            received_error_code = error_code
            error_received.set()

    client.set_handler_factory(Handler)

    async with client as connected_client:
        transport = await connected_client._current_transport()
        bad_client = MisbehavingRSocket(transport)

        setup = SetupFrame()
        setup.flags_lease = False
        setup.flags_resume = True
        setup.token_length = 1
        setup.resume_identification_token = b'a'
        setup.keep_alive_milliseconds = 123
        setup.max_lifetime_milliseconds = 456
        setup.data_encoding = WellKnownMimeTypes.APPLICATION_JSON.name.encode()
        setup.metadata_encoding = WellKnownMimeTypes.APPLICATION_JSON.name.encode()

        await bad_client.send_frame(setup)

        await error_received.wait()

        assert received_error_code == ErrorCode.UNSUPPORTED_SETUP


@pytest.mark.allow_error_log(regex_filter='Protocol error')
async def test_resume_request_unsupported(pipe_tcp: Tuple[RSocketServer, RSocketClient]):
    server, client = pipe_tcp

    received_error_code = None
    error_received = asyncio.Event()

    class Handler(BaseRequestHandler):
        async def on_error(self, error_code: ErrorCode, payload: Payload):
            nonlocal received_error_code
            received_error_code = error_code
            error_received.set()

    client.set_handler_using_factory(Handler)

    transport = await client._current_transport()
    bad_client = MisbehavingRSocket(transport)

    resume = ResumeFrame()
    resume.token_length = 1
    resume.resume_identification_token = b'a'
    resume.first_client_position = 123
    resume.last_server_position = 1234

    await bad_client.send_frame(resume)

    await error_received.wait()

    assert received_error_code == ErrorCode.REJECTED_RESUME
