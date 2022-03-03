import logging
from asyncio import Future

from quart import Quart

from rsocket.helpers import create_future
from rsocket.payload import Payload
from rsocket.request_handler import BaseRequestHandler
from rsocket.transports.quart_websocket import websocket_handler

app = Quart(__name__)


class Handler(BaseRequestHandler):

    async def request_response(self, payload: Payload) -> Future:
        return create_future(Payload(b'pong'))


@app.websocket("/")
async def ws():
    await websocket_handler(handler_factory=Handler)


if __name__ == "__main__":
    logging.basicConfig(level=logging.DEBUG)
    app.run(port=6565)
