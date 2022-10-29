import asyncio
import json
import logging

from reactivex import operators

from examples.tutorial.step10.models import Message
from rsocket.extensions.helpers import composite, route, metadata_item
from rsocket.extensions.mimetypes import WellKnownMimeTypes
from rsocket.frame_helpers import ensure_bytes
from rsocket.helpers import single_transport_provider
from rsocket.payload import Payload
from rsocket.reactivex.reactivex_client import ReactiveXClient
from rsocket.rsocket_client import RSocketClient
from rsocket.transports.tcp import TransportTCP


async def listen_for_messages(client, session_id):
    await ReactiveXClient(client).request_stream(Payload(metadata=composite(
        route('messages.incoming'),
        metadata_item(session_id, b'chat/session-id')
    ))).pipe(operators.do_action(on_next=lambda value: print(value.data), on_error=lambda exception: print(exception)))


async def main():
    connection = await asyncio.open_connection('localhost', 6565)

    async with RSocketClient(single_transport_provider(TransportTCP(*connection)),
                             metadata_encoding=WellKnownMimeTypes.MESSAGE_RSOCKET_COMPOSITE_METADATA) as client:
        session1 = (await login(client, 'user1')).data

        session2 = (await login(client, 'user2')).data
        #
        # task = asyncio.create_task(listen_for_messages(client, session1))
        #
        payload = Payload(ensure_bytes(json.dumps(Message('user2', 'some message').__dict__)),
                          composite(route('message'), metadata_item(session1, b'chat/session-id')))
        await client.request_response(payload)

        messages_done = asyncio.Event()
        # task.add_done_callback(lambda: messages_done.set())
        await asyncio.sleep(600)


async def login(client, username: str):
    payload = Payload(ensure_bytes(username), composite(route('login')))
    return await client.request_response(payload)


if __name__ == '__main__':
    logging.basicConfig(level=logging.DEBUG)
    asyncio.run(main())
