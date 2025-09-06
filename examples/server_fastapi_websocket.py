import uvicorn
from fastapi import FastAPI, WebSocket

from rsocket.helpers import create_future
from rsocket.local_typing import Awaitable
from rsocket.payload import Payload
from rsocket.request_handler import BaseRequestHandler
from rsocket.rsocket_server import RSocketServer
from rsocket.transports.http3_transport import Http3TransportWebsocket

app = FastAPI()


class Handler(BaseRequestHandler):

    async def request_response(self, payload: Payload) -> Awaitable[Payload]:
        return create_future(Payload(b'pong'))


@app.websocket("/")
async def endpoint(websocket: WebSocket):
    await websocket.accept()
    transport = Http3TransportWebsocket(websocket)
    RSocketServer(transport=transport, handler_factory=Handler)
    await transport.wait_for_disconnect()


if __name__ == "__main__":
    uvicorn.run(app, host="0.0.0.0", port=6565)
