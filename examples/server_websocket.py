import asyncio
import logging
from asyncio import Future

from aiohttp import web

from rsocket.payload import Payload
from rsocket.request_handler import BaseRequestHandler
from rsocket.transports.aiohttp_websocket import websocket_handler_factory


class Handler(BaseRequestHandler):

    async def request_response(self, payload: Payload) -> Future:
        future = asyncio.Future()
        future.set_result(Payload(b'pong'))
        return future


if __name__ == '__main__':
    logging.basicConfig(level=logging.DEBUG)
    app = web.Application()
    app.add_routes([web.get('/', websocket_handler_factory(handler_factory=Handler))])
    web.run_app(app, port=6565)
