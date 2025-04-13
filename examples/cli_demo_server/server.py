import asyncio
import logging

import asyncclick as click
import reactivex
from aiohttp import web
from reactivex import Observable

from rsocket.payload import Payload
from rsocket.routing.request_router import RequestRouter
from rsocket.routing.routing_request_handler import RoutingRequestHandler
from rsocket.rsocket_server import RSocketServer
from rsocket.transports.aiohttp_websocket import TransportAioHttpWebsocket
from rsocket.transports.tcp import TransportTCP

router = RequestRouter()


@router.response('echo')
async def echo(payload: Payload) -> Observable:
    return reactivex.just(Payload(payload.data))


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
@click.option('--transport', is_flag=False, default='tcp')
async def start_server(port: int, transport: str):
    logging.basicConfig(level=logging.DEBUG)

    logging.info(f'Starting {transport} server at localhost:{port}')

    if transport in ['ws']:
        app = web.Application()
        app.add_routes([web.get('/', websocket_handler_factory(
            handler_factory=lambda: RoutingRequestHandler(router)
        ))])

        await web._run_app(app, port=port)
    elif transport == 'tcp':
        def handle_client(reader, writer):
            RSocketServer(TransportTCP(reader, writer),
                          handler_factory=lambda: RoutingRequestHandler(router))

        server = await asyncio.start_server(handle_client, 'localhost', port)

        async with server:
            await server.serve_forever()
    else:
        raise Exception(f'Unsupported transport {transport}')


if __name__ == '__main__':
    start_server()
