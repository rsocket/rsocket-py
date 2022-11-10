import asyncio
import json
import logging
from asyncio import Event

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


def encode_dataclass(obj):
    return ensure_bytes(json.dumps(obj.__dict__))


async def login(client, username: str) -> bytes:
    payload = Payload(ensure_bytes(username), composite(route('login')))
    return (await client.request_response(payload)).data


def new_private_message(session_id: bytes, to_user: str, message: str) -> Payload:
    print(f'Sending {message} to user {to_user}')

    return Payload(encode_dataclass(Message(to_user, message)),
                   composite(route('message'), metadata_session_id(session_id)))


def new_file_upload(file_name: str, content: bytes) -> Payload:
    return Payload(content, composite(
        route('upload'),
        metadata_item(ensure_bytes(file_name), chat_filename_mimetype)
    ))


def new_file_download(file_name: str) -> Payload:
    return Payload(
        metadata=composite(route('download'), metadata_item(ensure_bytes(file_name), chat_filename_mimetype)))


def new_channel_message(session_id: bytes, to_channel: str, message: str) -> Payload:
    print(f'Sending {message} to channel {to_channel}')

    return Payload(encode_dataclass(Message(channel=to_channel, content=message)),
                   composite(route('message'), metadata_session_id(session_id)))


def metadata_session_id(session_id):
    return metadata_item(session_id, chat_session_mimetype)


async def join_channel(client, channel_name: str):
    join_request = Payload(ensure_bytes(channel_name), composite(route('join')))
    await client.request_response(join_request)


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


async def listen_for_statistics(client: RSocketClient, session_id, subscriber):
    client.request_channel(Payload(metadata=composite(
        route('statistics'),
        metadata_session_id(session_id)
    ))).subscribe(subscriber)

    await subscriber.done.wait()


async def main():
    connection1 = await asyncio.open_connection('localhost', 6565)

    async with RSocketClient(single_transport_provider(TransportTCP(*connection1)),
                             metadata_encoding=WellKnownMimeTypes.MESSAGE_RSOCKET_COMPOSITE_METADATA) as client1:
        connection2 = await asyncio.open_connection('localhost', 6565)

        async with RSocketClient(single_transport_provider(TransportTCP(*connection2)),
                                 metadata_encoding=WellKnownMimeTypes.MESSAGE_RSOCKET_COMPOSITE_METADATA) as client2:
            session1 = await login(client1, 'user1')
            await join_channel(client1, 'channel1')

            task = asyncio.create_task(listen_for_messages(client1, session1))

            statistics_handler = StatisticsHandler()
            statistics_task = asyncio.create_task(listen_for_statistics(client1, session1, statistics_handler))

            session2 = await login(client2, 'user2')
            await join_channel(client2, 'channel1')

            await client1.request_response(new_channel_message(session1, 'channel1', 'some message'))

            await client2.request_response(new_private_message(session2, 'user1', 'some message'))

            file_contents = b'abcdefg1234567'
            await client1.request_response(new_file_upload('file_name_1.txt', file_contents))

            download_request = Payload(metadata=composite(route('file_names')))

            file_list = await ReactiveXClient(client2).request_stream(
                download_request
            ).pipe(operators.map(lambda x: utf8_decode(x.data)),
                   operators.to_list())

            print(f'Filenames: {file_list}')

            download = await client2.request_response(new_file_download('file_name_1.txt'))

            if download.data != file_contents:
                raise Exception('File download failed')
            else:
                print(f'Downloaded file: {len(download.data)} bytes')

            messages_done = asyncio.Event()
            task.add_done_callback(lambda: messages_done.set())
            await messages_done.wait()


if __name__ == '__main__':
    logging.basicConfig(level=logging.INFO)
    asyncio.run(main())
