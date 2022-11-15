import asyncio
import json
import logging
from typing import List, Optional

from reactivex import operators

from examples.tutorial.step3.models import Message
from reactivestreams.subscriber import DefaultSubscriber
from reactivestreams.subscription import DefaultSubscription
from rsocket.extensions.helpers import composite, route
from rsocket.extensions.mimetypes import WellKnownMimeTypes
from rsocket.frame_helpers import ensure_bytes
from rsocket.helpers import single_transport_provider, utf8_decode
from rsocket.payload import Payload
from rsocket.reactivex.reactivex_client import ReactiveXClient
from rsocket.rsocket_client import RSocketClient
from rsocket.transports.tcp import TransportTCP


def encode_dataclass(obj):
    return ensure_bytes(json.dumps(obj.__dict__))


class ChatClient:
    def __init__(self, rsocket: RSocketClient):
        self._rsocket = rsocket
        self._session_id: Optional[str] = None

    async def login(self, username: str):
        payload = Payload(ensure_bytes(username), composite(route('login')))
        self._session_id = (await self._rsocket.request_response(payload)).data
        return self

    async def join(self, channel_name: str):
        request = Payload(ensure_bytes(channel_name), composite(route('channel.join')))
        await self._rsocket.request_response(request)
        return self

    async def leave(self, channel_name: str):
        request = Payload(ensure_bytes(channel_name), composite(route('channel.leave')))
        await self._rsocket.request_response(request)
        return self

    def listen_for_messages(self):
        def print_message(data):
            message = Message(**json.loads(data))
            print(f'{message.user} : {message.content}')

        class MessageListener(DefaultSubscriber, DefaultSubscription):
            def __init__(self):
                super().__init__()
                self.messages_done = asyncio.Event()

            def on_next(self, value, is_complete=False):
                print_message(value.data)

                if is_complete:
                    self.messages_done.set()

            def on_error(self, exception: Exception):
                print(exception)

            def cancel(self):
                self.subscription.cancel()

            def on_complete(self):
                self.messages_done.set()

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

    async def channel_message(self, channel: str, content: str):
        print(f'Sending {content} to channel {channel}')
        await self._rsocket.request_response(Payload(encode_dataclass(Message(channel=channel, content=content)),
                                                     composite(route('message'))))

    async def list_channels(self) -> List[str]:
        request = Payload(metadata=composite(route('channels')))
        return await ReactiveXClient(self._rsocket).request_stream(
            request
        ).pipe(operators.map(lambda x: utf8_decode(x.data)),
               operators.to_list())


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

            user1.listen_for_messages()
            user2.listen_for_messages()

            await user1.join('channel1')
            await user2.join('channel1')

            print(f'Channels: {await user1.list_channels()}')

            await user1.private_message('user2', 'private message from user1')
            await user1.channel_message('channel1', 'channel message from user1')

            await asyncio.sleep(3)

            user1.stop_listening_for_messages()
            user2.stop_listening_for_messages()

if __name__ == '__main__':
    logging.basicConfig(level=logging.INFO)
    asyncio.run(main())
