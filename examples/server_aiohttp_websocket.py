import logging
import sys
from asyncio import Future

from aiohttp import web

from rsocket.helpers import create_future
from rsocket.payload import Payload
from rsocket.request_handler import BaseRequestHandler
from rsocket.transports.aiohttp_websocket import websocket_handler_factory


class Handler(BaseRequestHandler):

    async def request_response(self, payload: Payload) -> Future:
        return create_future(Payload(b'pong'))


if __name__ == '__main__':
    port = sys.argv[1] if len(sys.argv) > 1 else 6565
    logging.basicConfig(level=logging.DEBUG)
    app = web.Application()
    app.add_routes([web.get('/', websocket_handler_factory(handler_factory=Handler))])
    web.run_app(app, port=port)
