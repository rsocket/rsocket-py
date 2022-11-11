import asyncio
import logging
import uuid
from dataclasses import dataclass, field
from typing import Dict, Optional, Awaitable

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


@dataclass(frozen=True)
class ChatData:
    session_state_map: Dict[str, UserSessionData] = field(default_factory=dict)


storage = ChatData()


class CustomRoutingRequestHandler(RoutingRequestHandler):
    def __init__(self, session: 'ChatUserSession', router: RequestRouter):
        super().__init__(router)
        self._session = session


class ChatUserSession:

    def __init__(self):
        self._session: Optional[UserSessionData] = None

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
