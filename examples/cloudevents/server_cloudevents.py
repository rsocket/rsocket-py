import asyncio
import json
import logging
import sys

from cloudevents.pydantic import CloudEvent

from rsocket.cloudevents.serialize import cloud_event_deserialize, cloud_event_serialize
from rsocket.routing.request_router import RequestRouter
from rsocket.routing.routing_request_handler import RoutingRequestHandler
from rsocket.rsocket_server import RSocketServer
from rsocket.transports.tcp import TransportTCP

router = RequestRouter(cloud_event_deserialize,
                       cloud_event_serialize)


@router.response('event')
async def event_response(event: CloudEvent) -> CloudEvent:
    return CloudEvent.create(attributes={
        'type': 'io.spring.event.Foo',
        'source': 'https://spring.io/foos'
    }, data=json.dumps(json.loads(event.data)))


def handler_factory():
    return RoutingRequestHandler(router)


async def run_server(server_port):
    logging.info('Starting server at localhost:%s', server_port)

    def session(*connection):
        RSocketServer(TransportTCP(*connection), handler_factory=handler_factory)

    async with await asyncio.start_server(session, 'localhost', server_port) as server:
        await server.serve_forever()


if __name__ == '__main__':
    port = sys.argv[1] if len(sys.argv) > 1 else 7000
    logging.basicConfig(level=logging.DEBUG)
    asyncio.run(run_server(port))
