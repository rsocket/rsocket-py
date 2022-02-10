import asyncio
from asyncio import Future

from rsocket.frame_builders import to_payload_frame
from rsocket.payload import Payload
from rsocket.request_handler import BaseRequestHandler
from tests.rsocket.misbehaving_rsocket import MisbehavingRSocket


async def test_send_frame_for_non_existing_stream(pipe_tcp, caplog):
    (client, server) = pipe_tcp
    done = asyncio.Event()

    class Handler(BaseRequestHandler):

        async def request_fire_and_forget(self, payload: Payload):
            done.set()

        async def request_response(self, payload: Payload) -> Future:
            future = asyncio.get_event_loop().create_future()
            future.set_result(Payload(b'response'))
            return future

    server.set_handler_using_factory(Handler)

    bad_client = MisbehavingRSocket(client._transport)

    client.fire_and_forget(Payload())

    await bad_client.send_frame(to_payload_frame(145, Payload()))

    await client.request_response(Payload(b'request'))

    await done.wait()

    records = caplog.get_records('call')
    dropped_frame_log = [record for record in records if 'Dropping frame from unknown stream 145' in record.message]
    assert len(dropped_frame_log) > 0
