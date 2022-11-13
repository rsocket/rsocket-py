import asyncio
import logging
from typing import Awaitable

from rsocket.frame_helpers import ensure_bytes
from rsocket.helpers import utf8_decode, create_response
from rsocket.payload import Payload
from rsocket.routing.request_router import RequestRouter
from rsocket.routing.routing_request_handler import RoutingRequestHandler
from rsocket.rsocket_server import RSocketServer
from rsocket.transports.tcp import TransportTCP


def handler_factory():
    router = RequestRouter()

    @router.response('login')
    async def login(payload: Payload) -> Awaitable[Payload]:
        username = utf8_decode(payload.data)

        logging.info(f'New user: {username}')

        return create_response(ensure_bytes(f'Hello {username}'))

    return RoutingRequestHandler(router)


async def run_server():
    def session(*connection):
        RSocketServer(TransportTCP(*connection), handler_factory=handler_factory)

    async with await asyncio.start_server(session, 'localhost', 6565) as server:
        await server.serve_forever()


if __name__ == '__main__':
    logging.basicConfig(level=logging.INFO)
    asyncio.run(run_server())
