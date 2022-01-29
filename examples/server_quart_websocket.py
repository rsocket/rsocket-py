import asyncio
from asyncio import Future

from quart import Quart

from rsocket.payload import Payload
from rsocket.request_handler import BaseRequestHandler
from rsocket.transports.quart_websocket import websocket_handler

app = Quart(__name__)


class Handler(BaseRequestHandler):

    async def request_response(self, payload: Payload) -> Future:
        future = asyncio.Future()
        future.set_result(Payload(b'pong'))
        return future


@app.websocket("/")
async def ws():
    await websocket_handler(handler_factory=Handler)


if __name__ == "__main__":
    app.run(port=6565)
