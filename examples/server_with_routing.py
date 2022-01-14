import asyncio
import logging
from datetime import timedelta

from examples.response_channel import ResponseChannel
from response_stream import ResponseStream
from rsocket.extensions.authentication import Authentication, AuthenticationSimple
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


@router.stream('stream')
async def stream_response(payload, composite_metadata):
    logging.info('Got stream request')
    return ResponseStream()


@router.fire_and_forget('no_response')
async def no_response(payload, composite_metadata):
    logging.info('No response sent to client')


@router.channel('channel')
async def channel_response(payload, composite_metadata):
    logging.info('Got channel request')
    channel = ResponseChannel()
    return channel, channel


@router.stream('stream-slow')
async def stream_slow(**kwargs):
    logging.info('Got slow stream request')
    return ResponseStream(delay_between_messages=timedelta(seconds=2))


async def authenticator(authentication: Authentication):
    if isinstance(authentication, AuthenticationSimple):
        if authentication.password != b'12345':
            raise Exception('Authentication error')
    else:
        raise Exception('Unsupported authentication')


def handler_factory(socket):
    return RoutingRequestHandler(socket, router, authenticator)


def handle_client(reader, writer):
    RSocketServer(reader, writer, handler_factory=handler_factory)


async def run_server():
    logging.basicConfig(level=logging.DEBUG)

    server = await asyncio.start_server(handle_client, 'localhost', 6565)

    async with server:
        await server.serve_forever()


asyncio.run(run_server())
