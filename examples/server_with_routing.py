import asyncio
import logging
import ssl
from dataclasses import dataclass
from datetime import timedelta
from typing import Optional

import asyncclick as click
from aiohttp import web

from examples.example_fixtures import large_data1
from examples.fixtures import generate_certificate_and_key
from examples.response_channel import response_stream_1, LoggingSubscriber
from response_stream import response_stream_2
from rsocket.extensions.authentication import Authentication, AuthenticationSimple
from rsocket.extensions.composite_metadata import CompositeMetadata
from rsocket.helpers import create_future
from rsocket.payload import Payload
from rsocket.routing.request_router import RequestRouter
from rsocket.routing.routing_request_handler import RoutingRequestHandler
from rsocket.rsocket_server import RSocketServer
from rsocket.transports.aiohttp_websocket import TransportAioHttpWebsocket
from rsocket.transports.tcp import TransportTCP

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


@router.response('large_data')
async def get_large_data():
    return create_future(Payload(large_data1))


@router.response('large_request')
async def get_large_data(payload: Payload):
    return create_future(Payload(payload.data))


@router.stream('stream')
async def stream_response(payload, composite_metadata):
    logging.info('Got stream request')
    return response_stream_1()


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


def handler_factory(socket):
    return RoutingRequestHandler(socket, router, authenticator)


def handle_client(reader, writer):
    RSocketServer(TransportTCP(reader, writer), handler_factory=handler_factory)


def websocket_handler_factory(**kwargs):
    async def websocket_handler(request):
        ws = web.WebSocketResponse()
        await ws.prepare(request)
        transport = TransportAioHttpWebsocket(ws)
        RSocketServer(transport, **kwargs)
        await transport.handle_incoming_ws_messages()
        return ws

    return websocket_handler


@click.command()
@click.option('--port', help='Port to listen on', default=6565, type=int)
@click.option('--with-ssl', is_flag=True, help='Enable SSL mode')
@click.option('--transport', is_flag=False, default='tcp')
async def start_server(with_ssl: bool, port: int, transport: str):
    logging.basicConfig(level=logging.DEBUG)

    logging.info(f'Starting {transport} server at localhost:{port}')

    if transport in ['ws', 'wss']:
        app = web.Application()
        app.add_routes([web.get('/', websocket_handler_factory(handler_factory=handler_factory))])

        with generate_certificate_and_key() as (certificate_path, key_path):
            if with_ssl:
                ssl_context = ssl.create_default_context(ssl.Purpose.CLIENT_AUTH)

                logging.info('Certificate %s', certificate_path)
                logging.info('Private-key %s', key_path)

                ssl_context.load_cert_chain(certificate_path, key_path)
            else:
                ssl_context = None

            await web._run_app(app, port=port, ssl_context=ssl_context)
    elif transport == 'tcp':

        server = await asyncio.start_server(handle_client, 'localhost', port)

        async with server:
            await server.serve_forever()
    else:
        raise Exception(f'Unsupported transport {transport}')


if __name__ == '__main__':
    start_server()
