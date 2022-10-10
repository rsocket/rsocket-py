import logging
import sys

from quart import Quart

from rsocket.helpers import create_future
from rsocket.local_typing import Awaitable
from rsocket.payload import Payload
from rsocket.request_handler import BaseRequestHandler
from rsocket.transports.quart_websocket import websocket_handler

app = Quart(__name__)


class Handler(BaseRequestHandler):

    async def request_response(self, payload: Payload) -> Awaitable[Payload]:
        return create_future(Payload(b'pong'))


@app.websocket("/")
async def ws():
    await websocket_handler(handler_factory=Handler)


if __name__ == "__main__":
    port = sys.argv[1] if len(sys.argv) > 1 else 6565
    logging.basicConfig(level=logging.DEBUG)
    app.run(port=port)
