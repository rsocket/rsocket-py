import asyncio
import json
import logging
from asyncio import Event
from typing import List

from reactivex import operators

from examples.tutorial.step10.models import Message, chat_session_mimetype, chat_filename_mimetype, ServerStatistics
from reactivestreams.publisher import DefaultPublisher
from reactivestreams.subscriber import DefaultSubscriber, Subscriber
from rsocket.extensions.helpers import composite, route, metadata_item
from rsocket.extensions.mimetypes import WellKnownMimeTypes
from rsocket.frame_helpers import ensure_bytes
from rsocket.helpers import single_transport_provider, utf8_decode
from rsocket.payload import Payload
from rsocket.reactivex.reactivex_client import ReactiveXClient
from rsocket.rsocket_client import RSocketClient
from rsocket.transports.tcp import TransportTCP


def encode_dataclass(obj):
    return ensure_bytes(json.dumps(obj.__dict__))


def metadata_session_id(session_id):
    return metadata_item(session_id, chat_session_mimetype)


class StatisticsHandler(DefaultPublisher, DefaultSubscriber):

    def __init__(self):
        super().__init__()
        self.done = Event()

    def subscribe(self, subscriber: Subscriber):
        super().subscribe(subscriber)

    def on_next(self, value: Payload, is_complete=False):
        statistics = ServerStatistics(**json.loads(utf8_decode(value.data)))
        print(statistics)

        if is_complete:
            self.done.set()


class ChatClient:
    def __init__(self, rsocket: RSocketClient):
        self._rsocket = rsocket
        self._listen_task = None
        self._session_id = None

    async def login(self, username: str):
        payload = Payload(ensure_bytes(username), composite(route('login')))
        self._session_id = (await self._rsocket.request_response(payload)).data
        return self

    async def join(self, channel_name: str):
        join_request = Payload(ensure_bytes(channel_name), composite(route('join')))
        await self._rsocket.request_response(join_request)
        return self

    def listen_for_messages(self):
        def print_message(data):
            message = Message(**json.loads(data))
            print(f'{message.user} ({message.channel}): {message.content}')

        async def listen_for_messages(client, session_id):
            await ReactiveXClient(client).request_stream(Payload(metadata=composite(
                route('messages.incoming'),
                metadata_session_id(session_id)
            ))).pipe(
                # operators.take(1),
                operators.do_action(on_next=lambda value: print_message(value.data),
                                    on_error=lambda exception: print(exception)))

        self._listen_task = asyncio.create_task(listen_for_messages(self._rsocket, self._session_id))

    async def wait_for_messages(self):
        messages_done = asyncio.Event()
        self._listen_task.add_done_callback(lambda _: messages_done.set())
        await messages_done.wait()

    def listen_for_statistics(self):
        async def listen_for_statistics(client: RSocketClient, session_id, subscriber):
            client.request_channel(Payload(metadata=composite(
                route('statistics'),
                metadata_session_id(session_id)
            ))).subscribe(subscriber)

            await subscriber.done.wait()

        statistics_handler = StatisticsHandler()
        self._statistics_task = asyncio.create_task(
            listen_for_statistics(self._rsocket, self._session_id, statistics_handler))

    async def private_message(self, username: str, content: str):
        print(f'Sending {content} to user {username}')
        await self._rsocket.request_response(Payload(encode_dataclass(Message(username, content)),
                                                     composite(route('message'), metadata_session_id(
                                                         self._session_id))))

    async def channel_message(self, channel: str, content: str):
        print(f'Sending {content} to channel {channel}')
        await self._rsocket.request_response(Payload(encode_dataclass(Message(channel=channel, content=content)),
                                                     composite(route('message'), metadata_session_id(
                                                         self._session_id))))

    async def upload(self, file_name, content):
        await self._rsocket.request_response(Payload(content, composite(
            route('upload'),
            metadata_item(ensure_bytes(file_name), chat_filename_mimetype)
        )))

    async def download(self, file_name):
        return await self._rsocket.request_response(Payload(
            metadata=composite(route('download'), metadata_item(ensure_bytes(file_name), chat_filename_mimetype))))

    async def list_files(self) -> List[str]:
        request = Payload(metadata=composite(route('file_names')))
        return await ReactiveXClient(self._rsocket).request_stream(
            request
        ).pipe(operators.map(lambda x: utf8_decode(x.data)),
               operators.to_list())

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

            user1.listen_for_statistics()

            print(f'Files: {await user1.list_files()}')
            print(f'Channels: {await user1.list_channels()}')

            await user1.private_message('user2', 'private message from user1')
            await user1.channel_message('channel1', 'channel message from user1')

            file_contents = b'abcdefg1234567'
            file_name = 'file_name_1.txt'
            await user1.upload(file_name, file_contents)

            download = await user2.download(file_name)

            if download.data != file_contents:
                raise Exception('File download failed')
            else:
                print(f'Downloaded file: {len(download.data)} bytes')

            await user1.wait_for_messages()


if __name__ == '__main__':
    logging.basicConfig(level=logging.INFO)
    asyncio.run(main())
