import asyncio
import logging
from dataclasses import dataclass, field
from typing import List, Dict

from rsocket.frame_helpers import str_to_bytes
from rsocket.helpers import create_future, utf8_decode
from rsocket.payload import Payload
from rsocket.routing.request_router import RequestRouter
from rsocket.routing.routing_request_handler import RoutingRequestHandler
from rsocket.rsocket_server import RSocketServer
from rsocket.transports.tcp import TransportTCP


@dataclass
class Storage:
    users: List[str] = field(default_factory=list)
    files: Dict[str, bytes] = field(default_factory=dict)


storage = Storage()
router = RequestRouter()


@router.response('login')
async def login(payload: Payload):
    username = utf8_decode(payload.data)
    logging.info(f'New user: {username}')

    storage.users.append(username)
    return create_future(Payload(str_to_bytes(f'Welcome to chat: {username}')))


def handler_factory(server):
    return RoutingRequestHandler(server, router)


async def run_server():
    def session(*connection):
        RSocketServer(TransportTCP(*connection), handler_factory=handler_factory)

    async with await asyncio.start_server(session, 'localhost', 6565) as server:
        await server.serve_forever()


if __name__ == '__main__':
    logging.basicConfig(level=logging.INFO)
    asyncio.run(run_server())
