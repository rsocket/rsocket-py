import asyncio
import logging
import uuid
from dataclasses import dataclass, field
from typing import Dict, Optional, Awaitable
from weakref import WeakValueDictionary

from rsocket.frame_helpers import ensure_bytes
from rsocket.helpers import utf8_decode, create_response
from rsocket.payload import Payload
from rsocket.routing.request_router import RequestRouter
from rsocket.routing.routing_request_handler import RoutingRequestHandler
from rsocket.rsocket_server import RSocketServer
from rsocket.transports.tcp import TransportTCP


class SessionId(str):  # allow weak reference
    pass


@dataclass(frozen=True)
class UserSessionData:
    username: str
    session_id: str


@dataclass(frozen=True)
class ChatData:
    user_session_by_id: Dict[str, UserSessionData] = field(default_factory=WeakValueDictionary)


chat_data = ChatData()


class ChatUserSession:

    def __init__(self):
        self._session: Optional[UserSessionData] = None

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

        return router


class CustomRoutingRequestHandler(RoutingRequestHandler):
    def __init__(self, session: ChatUserSession):
        super().__init__(session.router_factory())
        self._session = session


def handler_factory():
    return CustomRoutingRequestHandler(ChatUserSession())


async def run_server():
    def session(*connection):
        RSocketServer(TransportTCP(*connection), handler_factory=handler_factory)

    async with await asyncio.start_server(session, 'localhost', 6565) as server:
        await server.serve_forever()


if __name__ == '__main__':
    logging.basicConfig(level=logging.INFO)
    asyncio.run(run_server())
