import logging
import ssl

import asyncclick as click
from aiohttp import web

from examples.fixtures import cert_gen
from rsocket.helpers import create_future
from rsocket.local_typing import Awaitable
from rsocket.payload import Payload
from rsocket.request_handler import BaseRequestHandler
from rsocket.rsocket_server import RSocketServer
from rsocket.transports.aiohttp_websocket import TransportAioHttpWebsocket


class Handler(BaseRequestHandler):

    async def request_response(self, payload: Payload) -> Awaitable[Payload]:
        return create_future(Payload(b'pong'))


def websocket_handler_factory( **kwargs):
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
async def start_server(with_ssl: bool, port: int):
    logging.basicConfig(level=logging.DEBUG)
    app = web.Application()
    app.add_routes([web.get('/', websocket_handler_factory(handler_factory=Handler))])

    if with_ssl:
        ssl_context = ssl.create_default_context(ssl.Purpose.CLIENT_AUTH)

        with cert_gen() as (certificate, key):
            ssl_context.load_cert_chain(certificate, key)
    else:
        ssl_context = None

    await web._run_app(app, port=port, ssl_context=ssl_context)


if __name__ == '__main__':
    start_server()
