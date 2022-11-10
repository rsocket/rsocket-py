import asyncio
import json
import logging
import uuid
from asyncio import Queue
from collections import defaultdict
from dataclasses import dataclass, field
from typing import Dict, Optional, Set, Awaitable

from examples.tutorial.step10.models import (Message, chat_filename_mimetype, ClientStatistics, ServerStatisticsRequest,
                                             ServerStatistics)
from reactivestreams.publisher import DefaultPublisher
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
from rsocket.streams.stream_from_generator import StreamFromGenerator
from rsocket.transports.tcp import TransportTCP


@dataclass(frozen=True)
class Storage:
    channel_users: Dict[str, Set[str]] = field(default_factory=lambda: defaultdict(set))
    files: Dict[str, bytes] = field(default_factory=dict)
    channel_messages: Dict[str, Queue] = field(default_factory=lambda: defaultdict(Queue))


@dataclass(frozen=True)
class SessionState:
    username: str
    session_id: str
    messages: Queue = field(default_factory=Queue)
    statistics: Optional[ClientStatistics] = None


storage = Storage()

session_state_map: Dict[str, SessionState] = dict()


def ensure_channel(channel_name):
    if channel_name not in storage.channel_users:
        storage.channel_users[channel_name] = set()
        storage.channel_messages[channel_name] = Queue()
        asyncio.create_task(channel_message_delivery(channel_name))


async def channel_message_delivery(channel_name: str):
    logging.info('Starting channel delivery %s', channel_name)
    while True:
        try:
            message = await storage.channel_messages[channel_name].get()
            for session_id in storage.channel_users[channel_name]:
                user_specific_message = Message(user=message.user,
                                                content=message.content,
                                                channel=channel_name)
                session_state_map[session_id].messages.put_nowait(user_specific_message)
        except Exception as exception:
            logging.error(str(exception), exc_info=True)


def get_file_name(composite_metadata):
    return utf8_decode(composite_metadata.find_by_mimetype(chat_filename_mimetype)[0].content)


class CustomRoutingRequestHandler(RoutingRequestHandler):
    def __init__(self, session: 'ChatUserSession', router: RequestRouter):
        super().__init__(router)
        self._session = session

    async def on_close(self, rsocket, exception: Optional[Exception] = None):
        self._session.remove()
        return await super().on_close(rsocket, exception)


class ChatUserSession:

    def __init__(self):
        self._session: Optional[SessionState] = None

    def remove(self):
        print(f'Removing session: {self._session.session_id}')
        del session_state_map[self._session.session_id]

    def define_handler(self):
        router = RequestRouter()

        @router.response('login')
        async def login(payload: Payload) -> Awaitable[Payload]:
            username = utf8_decode(payload.data)
            logging.info(f'New user: {username}')
            session_id = str(uuid.uuid4())
            self._session = SessionState(username, session_id)
            session_state_map[session_id] = self._session

            return create_response(ensure_bytes(session_id))

        # @router.response('logout')
        # async def logout(payload: Payload, composite_metadata: CompositeMetadata):
        #     ...

        @router.response('join')
        async def join_channel(payload: Payload) -> Awaitable[Payload]:
            channel_name = payload.data.decode('utf-8')
            ensure_channel(channel_name)
            storage.channel_users[channel_name].add(self._session.session_id)
            return create_response()

        @router.response('leave')
        async def leave_channel(payload: Payload) -> Awaitable[Payload]:
            channel_name = payload.data.decode('utf-8')
            storage.channel_users[channel_name].discard(self._session.session_id)
            return create_response()

        @router.response('upload')
        async def upload_file(payload: Payload, composite_metadata: CompositeMetadata) -> Awaitable[Payload]:
            storage.files[get_file_name(composite_metadata)] = payload.data
            return create_response()

        @router.response('download')
        async def download_file(composite_metadata: CompositeMetadata) -> Awaitable[Payload]:
            file_name = get_file_name(composite_metadata)
            return create_response(storage.files[file_name],
                                   composite(metadata_item(ensure_bytes(file_name), chat_filename_mimetype)))

        @router.stream('file_names')
        async def get_file_names():
            count = len(storage.files)
            generator = ((Payload(ensure_bytes(file_name)), index == count) for (index, file_name) in
                         enumerate(storage.files.keys(), 1))
            return StreamFromGenerator(lambda: generator)

        @router.stream('channels')
        async def get_channels():
            count = len(storage.channel_messages)
            generator = ((Payload(ensure_bytes(channel)), index == count) for (index, channel) in
                         enumerate(storage.channel_messages.keys(), 1))
            return StreamFromGenerator(lambda: generator)

        @router.fire_and_forget('statistics')
        async def receive_statistics(payload: Payload):
            statistics = ClientStatistics(**json.loads(utf8_decode(payload.data)))
            self._session.statistics = statistics

        @router.channel('statistics')
        async def send_statistics(payload: Payload):

            class StatisticsChannel(DefaultPublisher, DefaultSubscriber, DefaultSubscription):

                def __init__(self, session: SessionState):
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
                        await asyncio.sleep(self._requested_statistics.period_seconds)
                        next_message = ServerStatistics(
                            user_count=len(session_state_map),
                            channel_count=len(storage.channel_messages)
                        )
                        next_payload = Payload(ensure_bytes(json.dumps(next_message.__dict__)))
                        self._subscriber.on_next(next_payload)

                def on_next(self, value: Payload, is_complete=False):
                    request = ServerStatisticsRequest(**json.loads(utf8_decode(value.data)))

                    if request.ids is not None:
                        self._requested_statistics.ids = request.ids

                    if request.period_seconds is not None:
                        self._requested_statistics.period_seconds = request.period_seconds

            response = StatisticsChannel(self._session)

            return response, response

        @router.response('message')
        async def send_message(payload: Payload) -> Awaitable[Payload]:
            message = Message(**json.loads(payload.data))

            if message.channel is not None:
                channel_message = Message(self._session.username, message.content, message.channel)
                await storage.channel_messages[message.channel].put(channel_message)
            elif message.user is not None:
                sessions = [session for session in session_state_map.values() if session.username == message.user]

                if len(sessions) > 0:
                    await sessions[0].messages.put(message)

            return create_response()

        @router.stream('messages.incoming')
        async def messages_incoming(composite_metadata: CompositeMetadata):
            class MessagePublisher(DefaultPublisher, DefaultSubscription):
                def __init__(self, session: SessionState):
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

            session_id = composite_metadata.find_by_mimetype(b'chat/session-id')[0].content.decode('utf-8')
            return MessagePublisher(session_state_map[session_id])

        return CustomRoutingRequestHandler(self, router)


def handler_factory():
    return ChatUserSession().define_handler()


async def run_server():
    def session(*connection):
        RSocketServer(TransportTCP(*connection), handler_factory=handler_factory)

    async with await asyncio.start_server(session, 'localhost', 6565) as server:
        await server.serve_forever()


if __name__ == '__main__':
    logging.basicConfig(level=logging.INFO)
    asyncio.run(run_server())