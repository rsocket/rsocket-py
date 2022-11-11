import asyncio
import json
import logging
import uuid
from asyncio import Queue
from dataclasses import dataclass, field
from typing import Dict, Optional, Awaitable

from more_itertools import first

from examples.tutorial.step2.models import (Message)
from reactivestreams.publisher import DefaultPublisher
from reactivestreams.subscriber import Subscriber
from reactivestreams.subscription import DefaultSubscription
from rsocket.extensions.composite_metadata import CompositeMetadata
from rsocket.frame_helpers import ensure_bytes
from rsocket.helpers import utf8_decode, create_response
from rsocket.payload import Payload
from rsocket.routing.request_router import RequestRouter
from rsocket.routing.routing_request_handler import RoutingRequestHandler
from rsocket.rsocket_server import RSocketServer
from rsocket.transports.tcp import TransportTCP


@dataclass(frozen=True)
class UserSessionData:
    username: str
    session_id: str
    messages: Queue = field(default_factory=Queue)


@dataclass(frozen=True)
class ChatData:
    session_state_map: Dict[str, UserSessionData] = field(default_factory=dict)


storage = ChatData()


class CustomRoutingRequestHandler(RoutingRequestHandler):
    def __init__(self, session: 'ChatUserSession', router: RequestRouter):
        super().__init__(router)
        self._session = session

    async def on_close(self, rsocket, exception: Optional[Exception] = None):
        self._session.remove()
        return await super().on_close(rsocket, exception)


def get_session_id(composite_metadata: CompositeMetadata):
    return utf8_decode(composite_metadata.find_by_mimetype(b'chat/session-id')[0].content)


def find_session_by_username(username: str) -> Optional[UserSessionData]:
    return first((session for session in storage.session_state_map.values() if
            session.username == username), None)


class ChatUserSession:

    def __init__(self):
        self._session: Optional[UserSessionData] = None

    def remove(self):
        print(f'Removing session: {self._session.session_id}')
        del storage.session_state_map[self._session.session_id]

    def define_handler(self):
        router = RequestRouter()

        @router.response('login')
        async def login(payload: Payload) -> Awaitable[Payload]:
            username = utf8_decode(payload.data)

            logging.info(f'New user: {username}')

            session_id = str(uuid.uuid4())
            self._session = UserSessionData(username, session_id)
            storage.session_state_map[session_id] = self._session

            return create_response(ensure_bytes(session_id))

        @router.response('message')
        async def send_message(payload: Payload) -> Awaitable[Payload]:
            message = Message(**json.loads(payload.data))

            session = find_session_by_username(message.user)
            
            await session.messages.put(message)

            return create_response()

        @router.stream('messages.incoming')
        async def messages_incoming(composite_metadata: CompositeMetadata):
            class MessagePublisher(DefaultPublisher, DefaultSubscription):
                def __init__(self, session: UserSessionData):
                    self._session = session
                    self._sender = None

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

            return MessagePublisher(storage.session_state_map[get_session_id(composite_metadata)])

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
