import asyncio
import logging
import uuid
from asyncio import Queue, Task
from collections import defaultdict
from dataclasses import dataclass, field
from typing import Dict, Optional, Set, Awaitable
from weakref import WeakValueDictionary, WeakSet

from examples.tutorial.step4.shared import (Message, dataclass_to_payload, decode_payload)
from reactivestreams.publisher import DefaultPublisher, Publisher
from reactivestreams.subscriber import Subscriber
from reactivestreams.subscription import DefaultSubscription
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


@dataclass(frozen=True)
class UserSessionData:
    username: str
    session_id: str
    messages: Queue = field(default_factory=Queue)


@dataclass(frozen=True)
class ChatData:
    channel_users: Dict[str, Set[str]] = field(default_factory=lambda: defaultdict(WeakSet))
    channel_messages: Dict[str, Queue] = field(default_factory=lambda: defaultdict(Queue))
    user_session_by_id: Dict[str, UserSessionData] = field(default_factory=WeakValueDictionary)


chat_data = ChatData()


def find_session_by_username(username: str) -> Optional[UserSessionData]:
    try:
        return next(session for session in chat_data.user_session_by_id.values() if
                    session.username == username)
    except StopIteration:
        return None


def ensure_channel_exists(channel_name: str):
    if channel_name not in chat_data.channel_users:
        chat_data.channel_users[channel_name] = set()
        chat_data.channel_messages[channel_name] = Queue()
        asyncio.create_task(channel_message_delivery(channel_name))


def find_username_by_session(session_id: SessionId) -> Optional[str]:
    session = chat_data.user_session_by_id.get(session_id)
    if session is None:
        return None
    return session.username


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


class ChatUserSession:

    def __init__(self):
        self._session: Optional[UserSessionData] = None

    def router_factory(self):
        router = RequestRouter(payload_mapper=decode_payload)

        @router.response('login')
        async def login(payload: Payload) -> Awaitable[Payload]:
            username = utf8_decode(payload.data)

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
                        logging.info('Canceling incoming message sender task')
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


async def run_server():
    def session(*connection):
        RSocketServer(TransportTCP(*connection),
                      handler_factory=handler_factory
                      )

    async with await asyncio.start_server(session, 'localhost', 6565) as server:
        await server.serve_forever()


if __name__ == '__main__':
    logging.basicConfig(level=logging.INFO)
    asyncio.run(run_server())
