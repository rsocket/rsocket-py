import asyncio
import logging
from typing import Optional

from examples.tutorial.step3.models import Message, encode_dataclass, decode_dataclass
from reactivestreams.subscriber import DefaultSubscriber
from reactivestreams.subscription import DefaultSubscription
from rsocket.extensions.helpers import composite, route
from rsocket.extensions.mimetypes import WellKnownMimeTypes
from rsocket.frame_helpers import ensure_bytes
from rsocket.helpers import single_transport_provider
from rsocket.payload import Payload
from rsocket.rsocket_client import RSocketClient
from rsocket.transports.tcp import TransportTCP


class ChatClient:
    def __init__(self, rsocket: RSocketClient):
        self._rsocket = rsocket
        self._message_subscriber: Optional = None
        self._session_id: Optional[str] = None
        self._username: Optional[str] = None

    async def login(self, username: str):
        self._username = username
        payload = Payload(ensure_bytes(username), composite(route('login')))
        self._session_id = (await self._rsocket.request_response(payload)).data
        return self

    def listen_for_messages(self):
        def print_message(data: bytes):
            message = decode_dataclass(data, Message)
            print(f'{self._username}: from {message.user}: {message.content}')

        class MessageListener(DefaultSubscriber, DefaultSubscription):

            def on_next(self, value, is_complete=False):
                print_message(value.data)

            def on_error(self, exception: Exception):
                print(exception)

            def cancel(self):
                self.subscription.cancel()

        self._message_subscriber = MessageListener()
        self._rsocket.request_stream(
            Payload(metadata=composite(route('messages.incoming')))
        ).subscribe(self._message_subscriber)

    def stop_listening_for_messages(self):
        self._message_subscriber.cancel()

    async def private_message(self, username: str, content: str):
        print(f'Sending {content} to user {username}')
        await self._rsocket.request_response(Payload(encode_dataclass(Message(username, content)),
                                                     composite(route('message'))))


async def main():
    connection1 = await asyncio.open_connection('localhost', 6565)

    async with RSocketClient(single_transport_provider(TransportTCP(*connection1)),
                             metadata_encoding=WellKnownMimeTypes.MESSAGE_RSOCKET_COMPOSITE_METADATA) as client1:
        connection2 = await asyncio.open_connection('localhost', 6565)

        async with RSocketClient(single_transport_provider(TransportTCP(*connection2)),
                                 metadata_encoding=WellKnownMimeTypes.MESSAGE_RSOCKET_COMPOSITE_METADATA) as client2:
            user1 = ChatClient(client1)
            user2 = ChatClient(client2)

            await user1.login('user1')
            await user2.login('user2')

            await messaging_example(user1, user2)


async def messaging_example(user1: ChatClient, user2: ChatClient):
    user1.listen_for_messages()
    user2.listen_for_messages()

    await user1.private_message('user2', 'private message from user1')

    await asyncio.sleep(1)

    user1.stop_listening_for_messages()
    user2.stop_listening_for_messages()


if __name__ == '__main__':
    logging.basicConfig(level=logging.INFO)
    asyncio.run(main())
