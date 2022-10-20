from starlette.applications import Starlette
from starlette.routing import WebSocketRoute
from starlette.types import Receive, Scope, Send
from starlette.websockets import WebSocket

from rsocket.helpers import noop
from rsocket.rsocket_server import RSocketServer
from rsocket.transports.http3_transport import Http3TransportWebsocket


async def web_socket(server_settings: dict, websocket: WebSocket, on_server_create=noop):
    await websocket.accept()
    transport = Http3TransportWebsocket(websocket)
    server = RSocketServer(transport=transport, **server_settings)
    on_server_create(server)
    await transport.wait_for_disconnect()


async def starlette_factory(server_settings, scope, receive, send, on_server_create=noop):
    async def web_socket_factory(websocket):
        return await web_socket(server_settings, websocket, on_server_create)

    starlette = Starlette(
        routes=[WebSocketRoute("/ws", web_socket_factory)]
    )

    await starlette(scope, receive, send)


class ApplicationFactory:

    def __init__(self, server_settings, on_server_create=noop):
        self._on_server_create = on_server_create
        self._server_settings = server_settings

    async def app(self, scope: Scope, receive: Receive, send: Send) -> None:
        await starlette_factory(self._server_settings, scope, receive, send, self._on_server_create)
