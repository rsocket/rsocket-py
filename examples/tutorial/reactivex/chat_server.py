import asyncio
import logging
import uuid
from asyncio import Queue
from collections import defaultdict
from dataclasses import dataclass, field
from typing import Dict, Optional, Set, Callable
from weakref import WeakValueDictionary, WeakSet

import reactivex
from more_itertools import first
from reactivex import Observable, operators, Subject, Observer

from examples.tutorial.reactivex.models import (Message, chat_filename_mimetype, ClientStatistics,
                                                ServerStatisticsRequest, ServerStatistics, dataclass_to_payload,
                                                decode_dataclass)
from rsocket.extensions.composite_metadata import CompositeMetadata
from rsocket.extensions.helpers import composite, metadata_item
from rsocket.frame_helpers import ensure_bytes
from rsocket.helpers import utf8_decode
from rsocket.payload import Payload
from rsocket.reactivex.back_pressure_publisher import (from_observable_with_backpressure, observable_from_queue,
                                                       observable_from_async_generator)
from rsocket.reactivex.reactivex_channel import ReactivexChannel
from rsocket.reactivex.reactivex_handler_adapter import reactivex_handler_factory
from rsocket.routing.request_router import RequestRouter
from rsocket.routing.routing_request_handler import RoutingRequestHandler
from rsocket.rsocket_server import RSocketServer
from rsocket.transports.tcp import TransportTCP


class SessionId(str):  # allow weak reference
    pass


@dataclass()
class UserSessionData:
    username: str
    session_id: str
    messages: Queue = field(default_factory=Queue)
    statistics: Optional[ClientStatistics] = None
    requested_statistics: ServerStatisticsRequest = field(default_factory=ServerStatisticsRequest)


@dataclass(frozen=True)
class ChatData:
    channel_users: Dict[str, Set[str]] = field(default_factory=lambda: defaultdict(WeakSet))
    files: Dict[str, bytes] = field(default_factory=dict)
    channel_messages: Dict[str, Queue] = field(default_factory=lambda: defaultdict(Queue))
    user_session_by_id: Dict[str, UserSessionData] = field(default_factory=WeakValueDictionary)


chat_data = ChatData()


def ensure_channel_exists(channel_name):
    if channel_name not in chat_data.channel_users:
        chat_data.channel_users[channel_name] = WeakSet()
        chat_data.channel_messages[channel_name] = Queue()
        asyncio.create_task(channel_message_delivery(channel_name))


async def channel_message_delivery(channel_name: str):
    logging.info('Starting channel delivery %s', channel_name)
    while True:
        try:
            message = await chat_data.channel_messages[channel_name].get()
            for session_id in chat_data.channel_users[channel_name]:
                user_specific_message = Message(user=message.user,
                                                content=message.content,
                                                channel=channel_name)
                chat_data.user_session_by_id[session_id].messages.put_nowait(user_specific_message)
        except Exception as exception:
            logging.error(str(exception), exc_info=True)


def get_file_name(composite_metadata):
    return utf8_decode(composite_metadata.find_by_mimetype(chat_filename_mimetype)[0].content)


def find_session_by_username(username: str) -> Optional[UserSessionData]:
    return first((session for session in chat_data.user_session_by_id.values() if
                  session.username == username), None)


def new_statistics_data(requested_statistics: ServerStatisticsRequest):
    statistics_data = {}

    if 'users' in requested_statistics.ids:
        statistics_data['user_count'] = len(chat_data.user_session_by_id)

    if 'channels' in requested_statistics.ids:
        statistics_data['channel_count'] = len(chat_data.channel_messages)

    return ServerStatistics(**statistics_data)


class ChatUserSession:

    def __init__(self):
        self._session: Optional[UserSessionData] = None

    def remove(self):
        print(f'Removing session: {self._session.session_id}')
        del chat_data.user_session_by_id[self._session.session_id]

    def router_factory(self):
        router = RequestRouter()

        @router.response('login')
        async def login(payload: Payload) -> Observable:
            username = utf8_decode(payload.data)
            logging.info(f'New user: {username}')
            session_id = SessionId(uuid.uuid4())
            self._session = UserSessionData(username, session_id)
            chat_data.user_session_by_id[session_id] = self._session

            return reactivex.just(Payload(ensure_bytes(session_id)))

        @router.response('channel.join')
        async def join_channel(payload: Payload) -> Observable:
            channel_name = utf8_decode(payload.data)
            ensure_channel_exists(channel_name)
            chat_data.channel_users[channel_name].add(self._session.session_id)
            return reactivex.empty()

        @router.response('channel.leave')
        async def leave_channel(payload: Payload) -> Observable:
            channel_name = utf8_decode(payload.data)
            chat_data.channel_users[channel_name].discard(self._session.session_id)
            return reactivex.empty()

        @router.response('file.upload')
        async def upload_file(payload: Payload, composite_metadata: CompositeMetadata) -> Observable:
            chat_data.files[get_file_name(composite_metadata)] = payload.data
            return reactivex.empty()

        @router.response('file.download')
        async def download_file(composite_metadata: CompositeMetadata) -> Observable:
            file_name = get_file_name(composite_metadata)
            return reactivex.just(Payload(chat_data.files[file_name],
                                          composite(metadata_item(ensure_bytes(file_name), chat_filename_mimetype))))

        @router.stream('files')
        async def get_file_names() -> Callable[[Subject], Observable]:
            async def generator():
                for file_name in chat_data.files.keys():
                    yield file_name

            return from_observable_with_backpressure(
                lambda backpressure: observable_from_async_generator(generator(), backpressure).pipe(
                    operators.map(lambda file_name: Payload(ensure_bytes(file_name)))
                ))

        @router.stream('channels')
        async def get_channels() -> Observable:
            return reactivex.from_iterable(
                (Payload(ensure_bytes(channel)) for channel in chat_data.channel_messages.keys()))

        @router.fire_and_forget('statistics')
        async def receive_statistics(payload: Payload):
            statistics = decode_dataclass(payload.data, ClientStatistics)

            logging.info('Received client statistics. memory usage: %s', statistics.memory_usage)

            self._session.statistics = statistics

        @router.channel('statistics')
        async def send_statistics() -> ReactivexChannel:

            async def statistics_generator():
                while True:
                    try:
                        await asyncio.sleep(self._session.requested_statistics.period_seconds)
                        yield new_statistics_data(self._session.requested_statistics)
                    except Exception:
                        logging.error('Statistics', exc_info=True)

            def on_next(payload: Payload):
                request = decode_dataclass(payload.data, ServerStatisticsRequest)

                logging.info(f'Received statistics request {request.ids}, {request.period_seconds}')

                if request.ids is not None:
                    self._session.requested_statistics.ids = request.ids

                if request.period_seconds is not None:
                    self._session.requested_statistics.period_seconds = request.period_seconds

            return ReactivexChannel(
                from_observable_with_backpressure(
                    lambda backpressure: observable_from_async_generator(
                        statistics_generator(), backpressure
                    ).pipe(
                        operators.map(dataclass_to_payload)
                    )),
                Observer(on_next=on_next),
                limit_rate=2)

        @router.response('message')
        async def send_message(payload: Payload) -> Observable:
            message = decode_dataclass(payload.data, Message)

            logging.info('Received message for user: %s, channel: %s', message.user, message.channel)

            target_message = Message(self._session.username, message.content, message.channel)

            if message.channel is not None:
                await chat_data.channel_messages[message.channel].put(target_message)
            elif message.user is not None:
                session = find_session_by_username(message.user)
                await session.messages.put(target_message)

            return reactivex.empty()

        @router.stream('messages.incoming')
        async def messages_incoming() -> Callable[[Subject], Observable]:
            return from_observable_with_backpressure(
                lambda backpressure: observable_from_queue(
                    self._session.messages, backpressure
                ).pipe(
                    operators.map(dataclass_to_payload)
                )
            )

        return router


class CustomRoutingRequestHandler(RoutingRequestHandler):
    def __init__(self, session: ChatUserSession):
        super().__init__(session.router_factory())
        self._session = session

    async def on_close(self, rsocket, exception: Optional[Exception] = None):
        self._session.remove()
        return await super().on_close(rsocket, exception)


def handler_factory():
    return CustomRoutingRequestHandler(ChatUserSession())


async def run_server():
    def session(*connection):
        RSocketServer(TransportTCP(*connection),
                      handler_factory=reactivex_handler_factory(handler_factory),
                      fragment_size_bytes=1_000_000)

    async with await asyncio.start_server(session, 'localhost', 6565) as server:
        await server.serve_forever()


if __name__ == '__main__':
    logging.basicConfig(level=logging.INFO)
    asyncio.run(run_server())
