import asyncio
import logging
from datetime import timedelta

from rsocket.payload import Payload
from rsocket.routing.request_router import RequestRouter
from rsocket.routing.routing_request_handler import RoutingRequestHandler
from rsocket.rsocket_server import RSocketServer

router = RequestRouter()


@router.response('single_request')
async def single_request_response(payload, composite_metadata):
    logging.info('Got single request')
    future = asyncio.Future()
    future.set_result(Payload(b'single_response'))
    return future


def handler_factory(socket):
    return RoutingRequestHandler(socket, router,
                                 lease_max_requests=5,
                                 lease_ttl=timedelta(seconds=2))


def handle_client(reader, writer):
    RSocketServer(reader, writer, handler_factory=handler_factory)


async def run_server():
    logging.basicConfig(level=logging.DEBUG)

    server = await asyncio.start_server(handle_client, 'localhost', 6565)

    async with server:
        await server.serve_forever()


asyncio.run(run_server())
