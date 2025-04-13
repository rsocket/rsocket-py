import asyncio
import logging
from typing import List, Optional

from examples.tutorial.step5.shared import Message, chat_filename_mimetype, encode_dataclass, decode_dataclass
from reactivestreams.subscriber import DefaultSubscriber
from reactivestreams.subscription import DefaultSubscription
from rsocket.awaitable.awaitable_rsocket import AwaitableRSocket
from rsocket.extensions.helpers import composite, route, metadata_item
from rsocket.extensions.mimetypes import WellKnownMimeTypes
from rsocket.frame_helpers import ensure_bytes
from rsocket.helpers import single_transport_provider, utf8_decode
from rsocket.payload import Payload
from rsocket.rsocket_client import RSocketClient
from rsocket.transports.tcp import TransportTCP


class ChatClient:
    def __init__(self, rsocket: RSocketClient):
        self._rsocket = rsocket
        self._username: Optional[str] = None

    async def login(self, username: str):
        self._username = username
        payload = Payload(ensure_bytes(username), composite(route('login')))
        response = await self._rsocket.request_response(payload)

        logging.info(f'Login response: {utf8_decode(response.data)}')

    async def join(self, channel_name: str):
        request = Payload(ensure_bytes(channel_name), composite(route('channel.join')))
        await self._rsocket.request_response(request)
        return self

    async def leave(self, channel_name: str):
        request = Payload(ensure_bytes(channel_name), composite(route('channel.leave')))
        await self._rsocket.request_response(request)
        return self

    def listen_for_messages(self):
        def print_message(data: bytes):
            message = decode_dataclass(data, Message)
            logging.info(f'to {self._username}: from {message.user} (channel: {message.channel}): {message.content}')

        class MessageListener(DefaultSubscriber, DefaultSubscription):

            def on_next(self, value, is_complete=False):
                print_message(value.data)

            def on_error(self, exception: Exception):
                logging.error(exception)

            def cancel(self):
                self.subscription.cancel()

        message_subscriber = MessageListener()
        self._rsocket.request_stream(
            Payload(metadata=composite(route('messages.incoming')))
        ).subscribe(message_subscriber)
        return message_subscriber

    async def private_message(self, username: str, content: str):
        logging.info(f'Sending "{content}" to user {username}')

        request = Payload(
            encode_dataclass(Message(username, content)),
            composite(route('message'))
        )

        await self._rsocket.request_response(request)

    async def channel_message(self, channel: str, content: str):
        logging.info(f'Sending "{content}" to channel {channel}')

        request = Payload(
            encode_dataclass(Message(channel=channel, content=content)),
            composite(route('message'))
        )

        await self._rsocket.request_response(request)

    async def upload(self, file_name, content):
        await self._rsocket.request_response(Payload(content, composite(
            route('file.upload'),
            metadata_item(ensure_bytes(file_name), chat_filename_mimetype)
        )))

    async def download(self, file_name):
        return await self._rsocket.request_response(Payload(
            metadata=composite(route('file.download'), metadata_item(ensure_bytes(file_name), chat_filename_mimetype))))

    async def list_files(self) -> List[str]:
        request = Payload(metadata=composite(route('files')))
        response = await AwaitableRSocket(self._rsocket).request_stream(request)
        return list(map(lambda _: utf8_decode(_.data), response))

    async def list_channels(self) -> List[str]:
        request = Payload(metadata=composite(route('channels')))
        response = await AwaitableRSocket(self._rsocket).request_stream(request)
        return list(map(lambda _: utf8_decode(_.data), response))

    async def list_channel_users(self, channel_name: str):
        request = Payload(ensure_bytes(channel_name), composite(route('channel.users')))
        response = await AwaitableRSocket(self._rsocket).request_stream(request)
        return list(map(lambda _: utf8_decode(_.data), response))


async def main():
    connection1 = await asyncio.open_connection('localhost', 6565)

    async with RSocketClient(single_transport_provider(TransportTCP(*connection1)),
                             metadata_encoding=WellKnownMimeTypes.MESSAGE_RSOCKET_COMPOSITE_METADATA,
                             fragment_size_bytes=1_000_000) as client1:
        connection2 = await asyncio.open_connection('localhost', 6565)

        async with RSocketClient(single_transport_provider(TransportTCP(*connection2)),
                                 metadata_encoding=WellKnownMimeTypes.MESSAGE_RSOCKET_COMPOSITE_METADATA,
                                 fragment_size_bytes=1_000_000) as client2:
            user1 = ChatClient(client1)
            user2 = ChatClient(client2)

            await user1.login('user1')
            await user2.login('user2')

            await messaging_example(user1, user2)
            await files_example(user1, user2)


async def messaging_example(user1: ChatClient, user2: ChatClient):
    message_subscriber1 = user1.listen_for_messages()
    message_subscriber2 = user2.listen_for_messages()

    channel_name = 'channel1'

    await user1.join(channel_name)
    await user2.join(channel_name)

    logging.info(f'Channels: {await user1.list_channels()}')
    logging.info(f'Channel: {channel_name} users: {await user1.list_channel_users(channel_name)}')

    await user1.private_message('user2', 'private message from user1')
    await user1.channel_message(channel_name, 'channel message from user1')

    await asyncio.sleep(1)

    await user1.leave(channel_name)
    logging.info(f'Channel {channel_name} users: {await user1.list_channel_users(channel_name)}')

    message_subscriber1.cancel()
    message_subscriber2.cancel()


async def files_example(user1: ChatClient, user2: ChatClient):
    file_contents = b'abcdefg1234567'
    file_name = 'file_name_1.txt'

    await user1.upload(file_name, file_contents)

    logging.info(f'Files: {await user1.list_files()}')

    download = await user2.download(file_name)

    if download.data != file_contents:
        raise Exception('File download failed')
    else:
        logging.info(f'Downloaded file: {len(download.data)} bytes')


if __name__ == '__main__':
    logging.basicConfig(level=logging.INFO)
    asyncio.run(main())
