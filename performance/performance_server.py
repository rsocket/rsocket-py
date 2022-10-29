import asyncio
import json
import logging
import sys
from dataclasses import dataclass
from datetime import timedelta
from typing import Optional

from rsocket.extensions.authentication import Authentication, AuthenticationSimple
from rsocket.extensions.composite_metadata import CompositeMetadata
from rsocket.helpers import create_future
from rsocket.payload import Payload
from rsocket.routing.request_router import RequestRouter
from rsocket.routing.routing_request_handler import RoutingRequestHandler
from rsocket.rsocket_server import RSocketServer
from rsocket.transports.tcp import TransportTCP
from performance.sample_responses import response_stream_2, response_stream_1, LoggingSubscriber

router = RequestRouter()


@dataclass
class Storage:
    last_metadata_push: Optional[bytes] = None
    last_fire_and_forget: Optional[bytes] = None


storage = Storage()


@router.response('single_request')
async def single_request_response(payload, composite_metadata):
    logging.info('Got single request')
    return create_future(Payload(b'single_response'))


@router.response('last_fnf')
async def get_last_fnf():
    logging.info('Got single request')
    return create_future(Payload(storage.last_fire_and_forget))


@router.response('last_metadata_push')
async def get_last_metadata_push():
    logging.info('Got single request')
    return create_future(Payload(storage.last_metadata_push))


@router.stream('stream')
async def stream_response(payload: Payload, composite_metadata):
    data = json.loads(payload.data)

    logging.info('Got stream request')

    return response_stream_1(**data)


@router.stream('fragmented_stream')
async def fragmented_stream(payload, composite_metadata):
    logging.info('Got fragmented stream request')
    return response_stream_2(fragment_size=6)


@router.fire_and_forget('no_response')
async def no_response(payload: Payload, composite_metadata):
    storage.last_fire_and_forget = payload.data
    logging.info('No response sent to client')


@router.metadata_push('metadata_push')
async def metadata_push(payload: Payload, composite_metadata: CompositeMetadata):
    for item in composite_metadata.items:
        if item.encoding == b'text/plain':
            storage.last_metadata_push = item.content


@router.channel('channel')
async def channel_response(payload, composite_metadata):
    logging.info('Got channel request')
    subscriber = LoggingSubscriber()
    channel = response_stream_1(local_subscriber=subscriber)
    return channel, subscriber


@router.stream('slow_stream')
async def stream_slow(**kwargs):
    logging.info('Got slow stream request')
    return response_stream_2(delay_between_messages=timedelta(seconds=2))


async def authenticator(route: str, authentication: Authentication):
    if isinstance(authentication, AuthenticationSimple):
        if authentication.password != b'12345':
            raise Exception('Authentication error')
    else:
        raise Exception('Unsupported authentication')


def handler_factory():
    return RoutingRequestHandler(router, authenticator)


def client_handler_factory(on_ready=None):
    def handle_client(reader, writer):
        RSocketServer(TransportTCP(reader, writer),
                      handler_factory=handler_factory,
                      fragment_size_bytes=64_000,
                      on_ready=on_ready
                      )

    return handle_client


async def run_server(server_port, on_ready=None):
    logging.info('Starting server at localhost:%s', server_port)

    server = await asyncio.start_server(client_handler_factory(on_ready), 'localhost', server_port)

    async with server:
        await server.serve_forever()


if __name__ == '__main__':
    port = sys.argv[1] if len(sys.argv) > 1 else 6565
    logging.basicConfig(level=logging.ERROR)
    asyncio.run(run_server(port))
