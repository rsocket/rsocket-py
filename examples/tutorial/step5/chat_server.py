import asyncio
import logging
import uuid
from asyncio import Queue, Task
from collections import defaultdict
from dataclasses import dataclass, field
from typing import Dict, Optional, Set, Awaitable
from weakref import WeakValueDictionary, WeakSet

from examples.tutorial.step5.shared import (Message, chat_filename_mimetype, dataclass_to_payload, decode_payload)
from reactivestreams.publisher import DefaultPublisher, Publisher
from reactivestreams.subscriber import Subscriber
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
from rsocket.streams.stream_from_generator import StreamFromGenerator
from rsocket.transports.tcp import TransportTCP


class SessionId(str):  # allow weak reference
    pass


@dataclass()
class UserSessionData:
    username: str
    session_id: SessionId
    messages: Queue = field(default_factory=Queue)


@dataclass(frozen=True)
class ChatData:
    channel_users: Dict[str, Set[SessionId]] = field(default_factory=lambda: defaultdict(WeakSet))
    files: Dict[str, bytes] = field(default_factory=dict)
    channel_messages: Dict[str, Queue] = field(default_factory=lambda: defaultdict(Queue))
    user_session_by_id: Dict[SessionId, UserSessionData] = field(default_factory=WeakValueDictionary)


chat_data = ChatData()


def ensure_channel_exists(channel_name: str):
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


def find_username_by_session(session_id: SessionId) -> Optional[str]:
    session = chat_data.user_session_by_id.get(session_id)
    if session is None:
        return None
    return session.username


class ChatUserSession:

    def __init__(self):
        self._session: Optional[UserSessionData] = None

    def router_factory(self):
        router = RequestRouter(payload_deserializer=decode_payload)

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

        @router.stream('channel.users')
        async def get_channel_users(channel_name: str) -> Publisher:
            if channel_name not in chat_data.channel_users:
                return EmptyStream()

            count = len(chat_data.channel_users[channel_name])
            generator = ((Payload(ensure_bytes(find_username_by_session(session_id))), index == count) for
                         (index, session_id) in
                         enumerate(chat_data.channel_users[channel_name], 1))

            return StreamFromGenerator(lambda: generator)

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
                    self._sender: Optional[Task] = None

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

        return router


class CustomRoutingRequestHandler(RoutingRequestHandler):
    def __init__(self, session: ChatUserSession):
        super().__init__(session.router_factory())
        self._session = session


def handler_factory():
    return CustomRoutingRequestHandler(ChatUserSession())


async def run_server():
    def session(*connection):
        RSocketServer(TransportTCP(*connection),
                      handler_factory=handler_factory,
                      fragment_size_bytes=1_000_000)

    async with await asyncio.start_server(session, 'localhost', 6565) as server:
        await server.serve_forever()


if __name__ == '__main__':
    logging.basicConfig(level=logging.INFO)
    asyncio.run(run_server())
