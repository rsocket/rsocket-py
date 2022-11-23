import asyncio
import logging
import resource
from asyncio import Task, Queue
from datetime import timedelta
from typing import List, Optional

from reactivex import operators

from examples.tutorial.reactivex.models import (Message, chat_filename_mimetype, ServerStatistics, ClientStatistics,
                                                ServerStatisticsRequest, encode_dataclass, dataclass_to_payload,
                                                decode_dataclass)
from rsocket.extensions.helpers import composite, route, metadata_item
from rsocket.extensions.mimetypes import WellKnownMimeTypes
from rsocket.frame_helpers import ensure_bytes
from rsocket.helpers import single_transport_provider, utf8_decode
from rsocket.payload import Payload
from rsocket.reactivex.back_pressure_publisher import observable_from_queue, from_observable_with_backpressure
from rsocket.reactivex.reactivex_client import ReactiveXClient
from rsocket.rsocket_client import RSocketClient
from rsocket.transports.tcp import TransportTCP


class StatisticsControl:
    def __init__(self):
        self.queue = Queue()

    def set_requested_statistics(self, ids: List[str]):
        self.queue.put_nowait(dataclass_to_payload(ServerStatisticsRequest(ids=ids)))

    def set_period(self, period: timedelta):
        self.queue.put_nowait(
            dataclass_to_payload(ServerStatisticsRequest(period_seconds=int(period.total_seconds()))))


class ChatClient:
    def __init__(self, rsocket: RSocketClient):
        self._rsocket = rsocket
        self._listen_task: Optional[Task] = None
        self._statistics_task: Optional[Task] = None
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

        async def listen_for_messages():
            await ReactiveXClient(self._rsocket).request_stream(Payload(metadata=composite(
                route('messages.incoming')
            ))).pipe(
                operators.do_action(on_next=lambda value: print_message(value.data),
                                    on_error=lambda exception: print(exception)))

        self._listen_task = asyncio.create_task(listen_for_messages())

    def stop_listening_for_messages(self):
        self._listen_task.cancel()

    async def send_statistics(self):
        memory_usage = resource.getrusage(resource.RUSAGE_SELF).ru_maxrss
        payload = Payload(encode_dataclass(ClientStatistics(memory_usage=memory_usage)),
                          metadata=composite(route('statistics')))
        await self._rsocket.fire_and_forget(payload)

    def listen_for_statistics(self) -> StatisticsControl:
        def print_statistics(value: bytes):
            statistics = decode_dataclass(value, ServerStatistics)
            print(f'users: {statistics.user_count}, channels: {statistics.channel_count}')

        control = StatisticsControl()

        async def listen_for_statistics():
            await ReactiveXClient(self._rsocket).request_channel(
                Payload(encode_dataclass(ServerStatisticsRequest(period_seconds=2)),
                        metadata=composite(
                            route('statistics')
                        )),
                observable=from_observable_with_backpressure(
                    lambda backpressure: observable_from_queue(control.queue, backpressure))
            ).pipe(
                operators.do_action(on_next=lambda value: print_statistics(value.data),
                                    on_error=lambda exception: print(exception)))

        self._statistics_task = asyncio.create_task(listen_for_statistics())

        return control

    def stop_listening_for_statistics(self):
        self._statistics_task.cancel()

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
        request = Payload(metadata=composite(
            route('file.download'),
            metadata_item(ensure_bytes(file_name), chat_filename_mimetype))
        )

        return await ReactiveXClient(self._rsocket).request_response(request).pipe(
            operators.map(lambda _: _.data),
            operators.last()
        )

    async def list_files(self) -> List[str]:
        request = Payload(metadata=composite(route('files')))
        return await ReactiveXClient(self._rsocket).request_stream(
            request
        ).pipe(operators.map(lambda _: utf8_decode(_.data)),
               operators.to_list())

    async def list_channels(self) -> List[str]:
        request = Payload(metadata=composite(route('channels')))
        return await ReactiveXClient(self._rsocket).request_stream(
            request
        ).pipe(operators.map(lambda _: utf8_decode(_.data)),
               operators.to_list())


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

    download_data = await user2.download(file_name)

    if download_data != file_contents:
        raise Exception('File download failed')
    else:
        print(f'Downloaded file: {len(download_data)} bytes')


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
