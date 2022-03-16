import asyncio
import logging
import sys
from datetime import timedelta

from rsocket.helpers import create_future
from rsocket.lease import SingleLeasePublisher
from rsocket.payload import Payload
from rsocket.routing.request_router import RequestRouter
from rsocket.routing.routing_request_handler import RoutingRequestHandler
from rsocket.rsocket_server import RSocketServer
from rsocket.transports.tcp import TransportTCP

router = RequestRouter()


@router.response('single_request')
async def single_request_response(payload, composite_metadata):
    logging.info('Got single request')
    return create_future(Payload(b'single_response'))


def handler_factory(socket):
    return RoutingRequestHandler(socket, router)


def handle_client(reader, writer):
    RSocketServer(TransportTCP(reader, writer), handler_factory=handler_factory, lease_publisher=SingleLeasePublisher(
        maximum_request_count=5,
        maximum_lease_time=timedelta(seconds=2)
    ))


async def run_server(server_port):
    server = await asyncio.start_server(handle_client, 'localhost', server_port)

    async with server:
        await server.serve_forever()


if __name__ == '__main__':
    port = sys.argv[1] if len(sys.argv) > 1 else 6565
    logging.basicConfig(level=logging.DEBUG)
    asyncio.run(run_server(port))
