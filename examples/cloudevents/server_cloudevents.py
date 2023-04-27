import asyncio
import json
import logging
import sys

from cloudevents.conversion import to_json, from_json
from cloudevents.pydantic import CloudEvent

from rsocket.helpers import create_future
from rsocket.payload import Payload
from rsocket.routing.request_router import RequestRouter
from rsocket.routing.routing_request_handler import RoutingRequestHandler
from rsocket.rsocket_server import RSocketServer
from rsocket.transports.tcp import TransportTCP

router = RequestRouter()


@router.response('event')
async def single_request_response(payload):
    received_event = from_json(CloudEvent, payload.data)
    received_data = json.loads(received_event.data)

    event = CloudEvent.create(attributes={
        'type': 'io.spring.event.Foo',
        'source': 'https://spring.io/foos'
    }, data=json.dumps(received_data))

    return create_future(Payload(to_json(event)))


def handler_factory():
    return RoutingRequestHandler(router)


async def run_server(server_port):
    logging.info('Starting server at localhost:%s', server_port)

    def session(*connection):
        RSocketServer(TransportTCP(*connection), handler_factory=handler_factory)

    server = await asyncio.start_server(session, 'localhost', server_port)

    async with server:
        await server.serve_forever()


if __name__ == '__main__':
    port = sys.argv[1] if len(sys.argv) > 1 else 7000
    logging.basicConfig(level=logging.DEBUG)
    asyncio.run(run_server(port))
