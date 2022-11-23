import asyncio
import logging
import resource
from asyncio import Event
from datetime import timedelta
from typing import List, Optional

from examples.tutorial.step6.models import (Message, chat_filename_mimetype, ServerStatistics, ClientStatistics,
                                            ServerStatisticsRequest, encode_dataclass, dataclass_to_payload,
                                            decode_dataclass)
from reactivestreams.publisher import DefaultPublisher
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


class StatisticsHandler(DefaultPublisher, DefaultSubscriber, DefaultSubscription):

    def __init__(self):
        super().__init__()
        self.done = Event()

    def on_next(self, payload: Payload, is_complete=False):
        statistics = decode_dataclass(payload.data, ServerStatistics)
        print(statistics)

        if is_complete:
            self.done.set()

    def cancel(self):
        self.subscription.cancel()

    def set_requested_statistics(self, ids: List[str]):
        self._subscriber.on_next(dataclass_to_payload(ServerStatisticsRequest(ids=ids)))

    def set_period(self, period: timedelta):
        self._subscriber.on_next(
            dataclass_to_payload(ServerStatisticsRequest(period_seconds=int(period.total_seconds()))))


class ChatClient:
    def __init__(self, rsocket: RSocketClient):
        self._rsocket = rsocket
        self._message_subscriber: Optional = None
        self._statistics_subscriber: Optional[StatisticsHandler] = None
        self._session_id: Optional[str] = None
        self._username: Optional[str] = None

    async def login(self, username: str):
        self._username = username
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
        def print_message(data: bytes):
            message = decode_dataclass(data, Message)
            print(f'{self._username}: from {message.user} ({message.channel}): {message.content}')

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

    async def send_statistics(self):
        memory_usage = resource.getrusage(resource.RUSAGE_SELF).ru_maxrss
        payload = Payload(encode_dataclass(ClientStatistics(memory_usage=memory_usage)),
                          metadata=composite(route('statistics')))
        await self._rsocket.fire_and_forget(payload)

    def listen_for_statistics(self) -> StatisticsHandler:
        self._statistics_subscriber = StatisticsHandler()
        self._rsocket.request_channel(Payload(metadata=composite(
            route('statistics')
        )), publisher=self._statistics_subscriber).subscribe(self._statistics_subscriber)
        return self._statistics_subscriber

    def stop_listening_for_statistics(self):
        self._statistics_subscriber.cancel()

    async def private_message(self, username: str, content: str):
        print(f'Sending {content} to user {username}')
        await self._rsocket.request_response(Payload(encode_dataclass(Message(username, content)),
                                                     composite(route('message'))))

    async def channel_message(self, channel: str, content: str):
        print(f'Sending {content} to channel {channel}')
        await self._rsocket.request_response(Payload(encode_dataclass(Message(channel=channel, content=content)),
                                                     composite(route('message'))))

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
            await statistics_example(user1)
            await files_example(user1, user2)


async def messaging_example(user1, user2):
    user1.listen_for_messages()
    user2.listen_for_messages()

    await user1.join('channel1')
    await user2.join('channel1')

    print(f'Channels: {await user1.list_channels()}')

    await user1.private_message('user2', 'private message from user1')
    await user1.channel_message('channel1', 'channel message from user1')

    await asyncio.sleep(1)

    user1.stop_listening_for_messages()
    user2.stop_listening_for_messages()


async def files_example(user1, user2):
    file_contents = b'abcdefg1234567'
    file_name = 'file_name_1.txt'

    await user1.upload(file_name, file_contents)

    print(f'Files: {await user1.list_files()}')

    download = await user2.download(file_name)

    if download.data != file_contents:
        raise Exception('File download failed')
    else:
        print(f'Downloaded file: {len(download.data)} bytes')


async def statistics_example(user1):
    await user1.send_statistics()

    statistics_control = user1.listen_for_statistics()

    await asyncio.sleep(5)

    statistics_control.set_requested_statistics(['users'])

    await asyncio.sleep(5)

    user1.stop_listening_for_statistics()


if __name__ == '__main__':
    logging.basicConfig(level=logging.INFO)
    asyncio.run(main())
