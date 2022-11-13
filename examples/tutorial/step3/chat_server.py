import asyncio
import json
import logging
import uuid
from asyncio import Queue
from collections import defaultdict
from dataclasses import dataclass, field
from typing import Dict, Optional, Set, Awaitable

from examples.tutorial.step5.models import (Message, chat_filename_mimetype, ClientStatistics)
from reactivestreams.publisher import DefaultPublisher, Publisher
from reactivestreams.subscriber import Subscriber
from reactivestreams.subscription import DefaultSubscription
from rsocket.frame_helpers import ensure_bytes
from rsocket.helpers import utf8_decode, create_response
from rsocket.payload import Payload
from rsocket.routing.request_router import RequestRouter
from rsocket.routing.routing_request_handler import RoutingRequestHandler
from rsocket.rsocket_server import RSocketServer
from rsocket.streams.stream_from_generator import StreamFromGenerator
from rsocket.transports.tcp import TransportTCP


@dataclass(frozen=True)
class UserSessionData:
    username: str
    session_id: str
    messages: Queue = field(default_factory=Queue)
    statistics: Optional[ClientStatistics] = None


@dataclass(frozen=True)
class ChatData:
    channel_users: Dict[str, Set[str]] = field(default_factory=lambda: defaultdict(set))
    channel_messages: Dict[str, Queue] = field(default_factory=lambda: defaultdict(Queue))
    user_session_by_id: Dict[str, UserSessionData] = field(default_factory=dict)


chat_data = ChatData()


def ensure_channel_exists(channel_name: str):
    if channel_name not in chat_data.channel_users:
        chat_data.channel_users[channel_name] = set()
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


class UserSession:

    def __init__(self):
        self._session: Optional[UserSessionData] = None

    def router_factory(self):
        router = RequestRouter()

        @router.response('login')
        async def login(payload: Payload) -> Awaitable[Payload]:
            username = utf8_decode(payload.data)
            logging.info(f'New user: {username}')
            session_id = str(uuid.uuid4())
            self._session = UserSessionData(username, session_id)
            chat_data.user_session_by_id[session_id] = self._session

            return create_response(ensure_bytes(session_id))

        @router.response('channel.join')
        async def join_channel(payload: Payload) -> Awaitable[Payload]:
            channel_name = payload.data.decode('utf-8')
            ensure_channel_exists(channel_name)
            chat_data.channel_users[channel_name].add(self._session.session_id)
            return create_response()

        @router.response('channel.leave')
        async def leave_channel(payload: Payload) -> Awaitable[Payload]:
            channel_name = payload.data.decode('utf-8')
            chat_data.channel_users[channel_name].discard(self._session.session_id)
            return create_response()

        @router.stream('channels')
        async def get_channels() -> Publisher:
            count = len(chat_data.channel_messages)
            generator = ((Payload(ensure_bytes(channel)), index == count) for (index, channel) in
                         enumerate(chat_data.channel_messages.keys(), 1))
            return StreamFromGenerator(lambda: generator)

        @router.response('message')
        async def send_message(payload: Payload) -> Awaitable[Payload]:
            message = Message(**json.loads(payload.data))

            if message.channel is not None:
                channel_message = Message(self._session.username, message.content, message.channel)
                await chat_data.channel_messages[message.channel].put(channel_message)
            elif message.user is not None:
                sessions = [session for session in chat_data.user_session_by_id.values() if
                            session.username == message.user]

                if len(sessions) > 0:
                    await sessions[0].messages.put(message)

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
                        next_payload = Payload(ensure_bytes(json.dumps(next_message.__dict__)))
                        self._subscriber.on_next(next_payload)

            return MessagePublisher(self._session)

        return router


class CustomRoutingRequestHandler(RoutingRequestHandler):
    def __init__(self, session: UserSession):
        super().__init__(session.router_factory())
        self._session = session


def handler_factory():
    return CustomRoutingRequestHandler(UserSession())


async def run_server():
    def session(*connection):
        RSocketServer(TransportTCP(*connection), handler_factory=handler_factory)

    async with await asyncio.start_server(session, 'localhost', 6565) as server:
        await server.serve_forever()


if __name__ == '__main__':
    logging.basicConfig(level=logging.INFO)
    asyncio.run(run_server())
