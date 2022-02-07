import asyncio
import logging
from datetime import timedelta

from examples.response_channel import response_stream_1, LoggingSubscriber
from response_stream import response_stream_2
from rsocket.extensions.authentication import Authentication, AuthenticationSimple
from rsocket.payload import Payload
from rsocket.routing.request_router import RequestRouter
from rsocket.routing.routing_request_handler import RoutingRequestHandler
from rsocket.rsocket_server import RSocketServer
from rsocket.transports.tcp import TransportTCP

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
    return response_stream_1()


@router.stream('fragmented_stream')
async def fragmented_stream(payload, composite_metadata):
    logging.info('Got fragmented stream request')
    return response_stream_2(fragment_size=6)


@router.fire_and_forget('no_response')
async def no_response(payload, composite_metadata):
    logging.info('No response sent to client')


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


def handler_factory(socket):
    return RoutingRequestHandler(socket, router, authenticator)


def handle_client(reader, writer):
    RSocketServer(TransportTCP(reader, writer), handler_factory=handler_factory)


async def run_server():
    logging.basicConfig(level=logging.DEBUG)

    server = await asyncio.start_server(handle_client, 'localhost', 6565)

    async with server:
        await server.serve_forever()


if __name__ == '__main__':
    asyncio.run(run_server())
