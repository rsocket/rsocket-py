import asyncio
from asyncio import Future

import pytest

from rsocket.exceptions import RSocketStreamIdInUse
from rsocket.frame_builders import to_payload_frame
from rsocket.helpers import create_future
from rsocket.payload import Payload
from rsocket.request_handler import BaseRequestHandler
from rsocket.stream_control import StreamControl
from tests.rsocket.misbehaving_rsocket import MisbehavingRSocket, UnknownFrame


async def test_send_frame_for_non_existing_stream(pipe_tcp, caplog):
    (client, server) = pipe_tcp
    done = asyncio.Event()

    class Handler(BaseRequestHandler):

        async def request_fire_and_forget(self, payload: Payload):
            done.set()

        async def request_response(self, payload: Payload) -> Future:
            return create_future(Payload(b'response'))

    server.set_handler_using_factory(Handler)

    bad_client = MisbehavingRSocket(client._transport)

    client.fire_and_forget(Payload())

    await bad_client.send_frame(to_payload_frame(145, Payload()))

    await client.request_response(Payload(b'request'))

    await done.wait()

    records = caplog.get_records('call')
    dropped_frame_log = [record for record in records if 'Dropping frame from unknown stream 145' in record.message]
    assert len(dropped_frame_log) > 0


@pytest.mark.allow_error_log(regex_filter='Error parsing frame')
async def test_send_frame_for_unknown_type(pipe_tcp, caplog):
    (client, server) = pipe_tcp

    class Handler(BaseRequestHandler):

        async def request_response(self, payload: Payload) -> Future:
            return create_future(Payload(b'response'))

    bad_client = MisbehavingRSocket(client._transport)
    server.set_handler_using_factory(Handler)

    frame = UnknownFrame()

    await bad_client.send_frame(frame)

    result = await client.request_response(Payload(b'request'))

    records = caplog.get_records('call')
    error_frame_log = [record for record in records if 'Error parsing frame' in record.message]

    assert len(error_frame_log) > 0
    assert result.data == b'response'


@pytest.mark.allow_error_log(regex_filter='Protocol error')
async def test_send_frame_for_stream_id_in_use(pipe_tcp, caplog):
    (client, server) = pipe_tcp

    class BrokenStreamControl(StreamControl):
        def assert_stream_id_available(self, stream_id: int):
            raise RSocketStreamIdInUse(stream_id)

    class Handler(BaseRequestHandler):

        async def request_response(self, payload: Payload) -> Future:
            await asyncio.sleep(2)
            return create_future(Payload(b'response'))

    server.set_handler_using_factory(Handler)
    server._stream_control = BrokenStreamControl(3)

    with pytest.raises(Exception):
        await client.request_response(Payload(b'request'))
