import asyncio
import logging
import uuid
from asyncio import Queue, Task
from collections import defaultdict
from dataclasses import dataclass, field
from typing import Dict, Optional, Set, Awaitable, Tuple
from weakref import WeakValueDictionary, WeakSet

from aiohttp import web

from examples.tutorial.step6.shared import (Message, chat_filename_mimetype, ClientStatistics, ServerStatisticsRequest,
                                            ServerStatistics, dataclass_to_payload, decode_dataclass, decode_payload)
from reactivestreams.publisher import DefaultPublisher, Publisher
from reactivestreams.subscriber import Subscriber, DefaultSubscriber
from reactivestreams.subscription import DefaultSubscription
from rsocket.extensions.composite_metadata import CompositeMetadata
from rsocket.extensions.helpers import composite, metadata_item
from rsocket.frame_helpers import ensure_bytes
from rsocket.helpers import utf8_decode, create_response
from rsocket.payload import Payload
from rsocket.routing.request_router import RequestRouter
from rsocket.routing.routing_request_handler import RoutingRequestHandler
from rsocket.rsocket_server import RSocketServer
from rsocket.streams.empty_stream import EmptyStream
from rsocket.streams.stream_from_async_generator import StreamFromAsyncGenerator
from rsocket.streams.stream_from_generator import StreamFromGenerator
from rsocket.transports.aiohttp_websocket import TransportAioHttpWebsocket


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
    try:
        return next(session for session in chat_data.user_session_by_id.values() if
                    session.username == username)
    except StopIteration:
        return None


def new_statistics_data(statistics_request: ServerStatisticsRequest):
    statistics_data = {}

    if 'users' in statistics_request.ids:
        statistics_data['user_count'] = len(chat_data.user_session_by_id)

    if 'channels' in statistics_request.ids:
        statistics_data['channel_count'] = len(chat_data.channel_messages)

    return ServerStatistics(**statistics_data)


def find_username_by_session(session_id: SessionId) -> Optional[str]:
    session = chat_data.user_session_by_id.get(session_id)
    if session is None:
        return None
    return session.username


class ChatUserSession:

    def __init__(self):
        self._session: Optional[UserSessionData] = None
        self._requested_statistics = ServerStatisticsRequest()

    def router_factory(self):
        router = RequestRouter(payload_mapper=decode_payload)

        @router.response('login')
        async def login(username: str) -> Awaitable[Payload]:
            logging.info(f'New user: {username}')
            session_id = SessionId(uuid.uuid4())
            self._session = UserSessionData(username, session_id)
            chat_data.user_session_by_id[session_id] = self._session

            return create_response(ensure_bytes(session_id))

        @router.response('channel.join')
        async def join_channel(channel_name: str) -> Awaitable[Payload]:
            ensure_channel_exists(channel_name)
            chat_data.channel_users[channel_name].add(self._session.session_id)
            return create_response()

        @router.response('channel.leave')
        async def leave_channel(channel_name: str) -> Awaitable[Payload]:
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
        async def receive_statistics(statistics: ClientStatistics):
            logging.info('Received client statistics. memory usage: %s', statistics.memory_usage)

            self._session.statistics = statistics

        @router.channel('statistics')
        async def send_statistics() -> Tuple[Optional[Publisher], Optional[Subscriber]]:

            async def statistics_generator():
                while True:
                    try:
                        await asyncio.sleep(self._requested_statistics.period_seconds)
                        next_message = new_statistics_data(self._requested_statistics)

                        yield dataclass_to_payload(next_message), False
                    except Exception:
                        logging.error('Statistics', exc_info=True)

            def on_next(payload: Payload, is_complete=False):
                request = decode_dataclass(payload.data, ServerStatisticsRequest)

                logging.info(f'Received statistics request {request.ids}, {request.period_seconds}')

                if request.ids is not None:
                    self._requested_statistics.ids = request.ids

                if request.period_seconds is not None:
                    self._requested_statistics.period_seconds = request.period_seconds

            return StreamFromAsyncGenerator(statistics_generator), DefaultSubscriber(on_next=on_next)

        @router.response('message')
        async def send_message(message: Message) -> Awaitable[Payload]:
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
                    if self._sender is not None:
                        logging.info('Canceling message sender task')
                        self._sender.cancel()
                        self._sender = None

                def subscribe(self, subscriber: Subscriber):
                    super(MessagePublisher, self).subscribe(subscriber)
                    subscriber.on_subscribe(self)
                    self._sender = asyncio.create_task(self._message_sender())

                async def _message_sender(self):
                    while True:
                        next_message = await self._session.messages.get()
                        self._subscriber.on_next(dataclass_to_payload(next_message))

            return MessagePublisher(self._session)

        @router.stream('channel.users')
        async def get_channel_users(channel_name: str) -> Publisher:
            if channel_name not in chat_data.channel_users:
                return EmptyStream()

            count = len(chat_data.channel_users[channel_name])
            generator = ((Payload(ensure_bytes(find_username_by_session(session_id))), index == count) for
                         (index, session_id) in
                         enumerate(chat_data.channel_users[channel_name], 1))

            return StreamFromGenerator(lambda: generator)

        return router


class CustomRoutingRequestHandler(RoutingRequestHandler):
    def __init__(self, session: ChatUserSession):
        super().__init__(session.router_factory())
        self._session = session


def handler_factory():
    return CustomRoutingRequestHandler(ChatUserSession())


def start_server():
    def websocket_handler_factory(**kwargs):
        async def websocket_handler(request):
            websocket = web.WebSocketResponse()
            await websocket.prepare(request)

            transport = TransportAioHttpWebsocket(websocket)
            RSocketServer(transport, **kwargs)
            await transport.handle_incoming_ws_messages()

            return websocket

        return websocket_handler

    app = web.Application()
    app.add_routes([web.get('/chat', websocket_handler_factory(handler_factory=handler_factory,
                                                               fragment_size_bytes=1_000_000))])

    web.run_app(app, port=6565)


if __name__ == '__main__':
    logging.basicConfig(level=logging.INFO)
    start_server()
