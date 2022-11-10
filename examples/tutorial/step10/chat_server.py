import asyncio
import json
import logging
import uuid
from asyncio import Queue
from collections import defaultdict
from dataclasses import dataclass, field
from typing import Dict, Optional, Set

from examples.tutorial.step10.models import Message
from reactivestreams.publisher import DefaultPublisher
from reactivestreams.subscriber import Subscriber
from reactivestreams.subscription import DefaultSubscription
from rsocket.extensions.composite_metadata import CompositeMetadata
from rsocket.frame_helpers import ensure_bytes
from rsocket.helpers import create_future, utf8_decode
from rsocket.payload import Payload
from rsocket.routing.request_router import RequestRouter
from rsocket.routing.routing_request_handler import RoutingRequestHandler
from rsocket.rsocket_server import RSocketServer
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
        async def login(payload: Payload):
            username = utf8_decode(payload.data)
            logging.info(f'New user: {username}')
            session_id = str(uuid.uuid4())
            self._session = SessionState(username, session_id)
            session_state_map[session_id] = self._session

            return create_future(Payload(ensure_bytes(session_id)))

        @router.response('logout')
        async def logout(payload: Payload, composite_metadata: CompositeMetadata):
            ...

        @router.response('join')
        async def join_channel(payload: Payload):
            channel_name = payload.data.decode('utf-8')
            ensure_channel(channel_name)
            storage.channel_users[channel_name].add(self._session.session_id)
            return create_future(Payload())

        @router.response('leave')
        async def leave_channel(payload: Payload):
            channel_name = payload.data.decode('utf-8')
            storage.channel_users[channel_name].discard(self._session.session_id)
            return create_future(Payload())

        @router.response('upload')
        async def upload_file(payload: Payload):
            ...

        @router.response('download')
        async def download_file(payload: Payload):
            ...

        @router.fire_and_forget('statistics')
        async def statistics(payload: Payload):
            ...

        @router.metadata_push('download.priority')
        async def download_priority(payload: Payload):
            ...

        @router.response('message')
        async def send_message(payload: Payload, composite_metadata: CompositeMetadata):
            message = Message(**json.loads(payload.data))

            if message.channel is not None:
                channel_message = Message(self._session.username, message.content, message.channel)
                await storage.channel_messages[message.channel].put(channel_message)
            elif message.user is not None:
                sessions = [session for session in session_state_map.values() if session.username == message.user]

                if len(sessions) > 0:
                    await sessions[0].messages.put(message)

            return create_future(Payload())

        @router.stream('messages.incoming')
        async def messages_incoming(payload: Payload, composite_metadata: CompositeMetadata):
            class MessagePublisher(DefaultPublisher, DefaultSubscription):
                def __init__(self, state: SessionState):
                    self._state = state

                def cancel(self):
                    self._sender.cancel()

                def subscribe(self, subscriber: Subscriber):
                    super(MessagePublisher, self).subscribe(subscriber)
                    subscriber.on_subscribe(self)
                    self._sender = asyncio.create_task(self._message_sender())

                async def _message_sender(self):
                    while True:
                        next_message = await self._state.messages.get()
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
