import asyncio
import json
import logging
import uuid
from asyncio import Queue
from collections import defaultdict
from dataclasses import dataclass, field
from typing import Dict, Optional

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
    channels: Dict[str, Queue] = field(default_factory=lambda: defaultdict(Queue))
    private: Dict[str, Queue] = field(default_factory=lambda: defaultdict(Queue))
    files: Dict[str, bytes] = field(default_factory=dict)


@dataclass(frozen=True)
class SessionState:
    username: str
    session_id: str


storage = Storage()

session_state_map: Dict[str, SessionState] = dict()


class ChatUserSession:

    def __init__(self):
        self._session: Optional[SessionState] = None

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
        async def logout(payload, composite_metadata: CompositeMetadata):
            ...

        @router.response('join')
        async def join_channel():
            ...

        @router.response('leave')
        async def leave_channel():
            ...

        @router.response('upload')
        async def upload_file():
            ...

        @router.response('download')
        async def download_file():
            ...

        @router.fire_and_forget('statistics')
        async def statistics():
            ...

        @router.metadata_push('download.priority')
        async def download_priority():
            ...

        @router.response('message')
        async def send_message(payload: Payload, composite_metadata: CompositeMetadata):
            message = Message(**json.loads(payload.data))
            storage.private[message.user].put_nowait(message)
            return create_future()

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
                        next_message = await storage.private[self._state.username].get()
                        self._subscriber.on_next(Payload(ensure_bytes(json.dumps(next_message.__dict__))))

            session_id = composite_metadata.find_by_mimetype(b'chat/session-id')[0].content.decode('utf-8')
            return MessagePublisher(session_state_map[session_id])

        @router.channel('messages.bidirectional')
        async def messages_bidirectional():
            ...

        return RoutingRequestHandler(router)


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
