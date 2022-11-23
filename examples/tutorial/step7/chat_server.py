import asyncio
import logging
import uuid
from asyncio import Queue
from collections import defaultdict
from dataclasses import dataclass, field
from typing import Dict, Optional, Set, Awaitable, Tuple
from weakref import WeakValueDictionary, WeakSet

from more_itertools import first

from examples.tutorial.step6.models import (Message, chat_filename_mimetype, ClientStatistics, ServerStatisticsRequest,
                                            ServerStatistics, dataclass_to_payload, decode_dataclass)
from reactivestreams.publisher import DefaultPublisher, Publisher
from reactivestreams.subscriber import Subscriber, DefaultSubscriber
from reactivestreams.subscription import DefaultSubscription
from rsocket.extensions.composite_metadata import CompositeMetadata
from rsocket.extensions.helpers import composite, metadata_item, route
from rsocket.frame_helpers import ensure_bytes
from rsocket.helpers import utf8_decode, create_response
from rsocket.payload import Payload
from rsocket.routing.request_router import RequestRouter
from rsocket.routing.routing_request_handler import RoutingRequestHandler
from rsocket.rsocket_server import RSocketServer
from rsocket.streams.stream_from_generator import StreamFromGenerator
from rsocket.transports.tcp import TransportTCP


class SessionId(str):  # allow weak reference
    pass


@dataclass()
class UserSessionData:
    username: str
    session_id: str
    messages: Queue = field(default_factory=Queue)
    statistics: Optional[ClientStatistics] = None


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


class ChatUserSession:

    def __init__(self):
        self._session: Optional[UserSessionData] = None

    def remove(self):
        print(f'Removing session: {self._session.session_id}')
        del chat_data.user_session_by_id[self._session.session_id]

    def router_factory(self):
        router = RequestRouter()

        @router.response('login')
        async def login(payload: Payload) -> Awaitable[Payload]:
            username = utf8_decode(payload.data)
            logging.info(f'New user: {username}')
            session_id = SessionId(uuid.uuid4())
            self._session = UserSessionData(username, session_id)
            chat_data.user_session_by_id[session_id] = self._session

            return create_response(ensure_bytes(session_id))

        @router.response('channel.join')
        async def join_channel(payload: Payload) -> Awaitable[Payload]:
            channel_name = utf8_decode(payload.data)
            ensure_channel_exists(channel_name)
            chat_data.channel_users[channel_name].add(self._session.session_id)
            return create_response()

        @router.response('channel.leave')
        async def leave_channel(payload: Payload) -> Awaitable[Payload]:
            channel_name = utf8_decode(payload.data)
            chat_data.channel_users[channel_name].discard(self._session.session_id)
            return create_response()

        @router.response('file.upload')
        async def upload_file(payload: Payload, composite_metadata: CompositeMetadata) -> Awaitable[Payload]:
            chat_data.files[get_file_name(composite_metadata)] = payload.data
            return create_response()

        @router.response('file.download')
        async def download_file(composite_metadata: CompositeMetadata) -> Awaitable[Payload]:
            file_name = get_file_name(composite_metadata)
            return create_response(chat_data.files[file_name],
                                   composite(metadata_item(ensure_bytes(file_name), chat_filename_mimetype)))

        @router.stream('files')
        async def get_file_names() -> Publisher:
            count = len(chat_data.files)
            generator = ((Payload(ensure_bytes(file_name)), index == count) for (index, file_name) in
                         enumerate(chat_data.files.keys(), 1))
            return StreamFromGenerator(lambda: generator)

        @router.stream('channels')
        async def get_channels() -> Publisher:
            count = len(chat_data.channel_messages)
            generator = ((Payload(ensure_bytes(channel)), index == count) for (index, channel) in
                         enumerate(chat_data.channel_messages.keys(), 1))
            return StreamFromGenerator(lambda: generator)

        @router.fire_and_forget('statistics')
        async def receive_statistics(payload: Payload):
            statistics = decode_dataclass(payload.data, ClientStatistics)

            logging.info('Received client statistics. memory usage: %s', statistics.memory_usage)

            self._session.statistics = statistics

        @router.channel('statistics')
        async def send_statistics() -> Tuple[Optional[Publisher], Optional[Subscriber]]:

            class StatisticsChannel(DefaultPublisher, DefaultSubscriber, DefaultSubscription):

                def __init__(self, session: UserSessionData):
                    super().__init__()
                    self._session = session
                    self._requested_statistics = ServerStatisticsRequest()

                def cancel(self):
                    self._sender.cancel()

                def subscribe(self, subscriber: Subscriber):
                    super().subscribe(subscriber)
                    subscriber.on_subscribe(self)
                    self._sender = asyncio.create_task(self._statistics_sender())

                async def _statistics_sender(self):
                    while True:
                        try:
                            await asyncio.sleep(self._requested_statistics.period_seconds)
                            next_message = self.new_statistics_data()

                            self._subscriber.on_next(dataclass_to_payload(next_message))
                        except Exception:
                            logging.error('Statistics', exc_info=True)

                def new_statistics_data(self):
                    statistics_data = {}

                    if 'users' in self._requested_statistics.ids:
                        statistics_data['user_count'] = len(chat_data.user_session_by_id)

                    if 'channels' in self._requested_statistics.ids:
                        statistics_data['channel_count'] = len(chat_data.channel_messages)

                    return ServerStatistics(**statistics_data)

                def on_next(self, payload: Payload, is_complete=False):
                    request = decode_dataclass(payload.data, ServerStatisticsRequest)

                    logging.info(f'Received statistics request {request.ids}, {request.period_seconds}')

                    if request.ids is not None:
                        self._requested_statistics.ids = request.ids

                    if request.period_seconds is not None:
                        self._requested_statistics.period_seconds = request.period_seconds

            response = StatisticsChannel(self._session)

            return response, response

        @router.response('message')
        async def send_message(payload: Payload) -> Awaitable[Payload]:
            message = decode_dataclass(payload.data, Message)

            logging.info('Received message for user: %s, channel: %s', message.user, message.channel)

            target_message = Message(self._session.username, message.content, message.channel)

            if message.channel is not None:
                await chat_data.channel_messages[message.channel].put(target_message)
            elif message.user is not None:
                session = find_session_by_username(message.user)
                await session.messages.put(target_message)

            return create_response()

        @router.stream('messages.incoming')
        async def messages_incoming() -> Publisher:
            class MessagePublisher(DefaultPublisher, DefaultSubscription):
                def __init__(self, session: UserSessionData):
                    self._session = session

                def cancel(self):
                    self._sender.cancel()

                def subscribe(self, subscriber: Subscriber):
                    super(MessagePublisher, self).subscribe(subscriber)
                    subscriber.on_subscribe(self)
                    self._sender = asyncio.create_task(self._message_sender())

                async def _message_sender(self):
                    while True:
                        next_message = await self._session.messages.get()
                        self._subscriber.on_next(dataclass_to_payload(next_message))

            return MessagePublisher(self._session)

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
    async def session(*connection):
        server = RSocketServer(TransportTCP(*connection),
                               handler_factory=handler_factory,
                               fragment_size_bytes=1_000_000)

        response = await server.request_response(Payload(metadata=composite(route('time'))))
        print(f'Client time: {response.data}')

    async with await asyncio.start_server(session, 'localhost', 6565) as server:
        await server.serve_forever()


if __name__ == '__main__':
    logging.basicConfig(level=logging.INFO)
    asyncio.run(run_server())
