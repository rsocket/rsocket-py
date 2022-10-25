import logging
import ssl
import sys

from aiohttp import web

from examples.fixtures import cert_gen
from rsocket.helpers import create_future
from rsocket.local_typing import Awaitable
from rsocket.payload import Payload
from rsocket.request_handler import BaseRequestHandler
from rsocket.transports.aiohttp_websocket import websocket_handler_factory


class Handler(BaseRequestHandler):

    async def request_response(self, payload: Payload) -> Awaitable[Payload]:
        return create_future(Payload(b'pong'))


if __name__ == '__main__':
    port = sys.argv[1] if len(sys.argv) > 1 else 6565
    logging.basicConfig(level=logging.DEBUG)
    app = web.Application()
    app.add_routes([web.get('/', websocket_handler_factory(handler_factory=Handler))])

    ssl_context = ssl.create_default_context(ssl.Purpose.CLIENT_AUTH)

    with cert_gen() as (certificate, key):
        ssl_context.load_cert_chain(certificate, key)

        web.run_app(app, port=port, ssl_context=ssl_context)
