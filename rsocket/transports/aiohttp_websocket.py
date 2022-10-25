import asyncio
from contextlib import asynccontextmanager
from typing import Optional

import aiohttp
from aiohttp import web, ClientWebSocketResponse

from rsocket.exceptions import RSocketTransportError
from rsocket.frame import Frame
from rsocket.helpers import wrap_transport_exception, single_transport_provider
from rsocket.logger import logger
from rsocket.rsocket_client import RSocketClient
from rsocket.rsocket_server import RSocketServer
from rsocket.transports.abstract_messaging import AbstractMessagingTransport


@asynccontextmanager
async def websocket_client(url: Optional[str] = None,
                           websocket: Optional[ClientWebSocketResponse] = None,
                           **kwargs) -> RSocketClient:
    async with RSocketClient(single_transport_provider(TransportAioHttpClient(url, websocket)),
                             **kwargs) as client:
        yield client


def websocket_handler_factory(on_server_create=None, **kwargs):
    async def websocket_handler(request):
        ws = web.WebSocketResponse()
        await ws.prepare(request)
        transport = TransportAioHttpWebsocket(ws)
        server = RSocketServer(transport, **kwargs)

        if on_server_create is not None:
            on_server_create(server)

        await transport.handle_incoming_ws_messages()
        return ws

    return websocket_handler


class TransportAioHttpClient(AbstractMessagingTransport):

    def __init__(self, url: Optional[str] = None, websocket: Optional[ClientWebSocketResponse] = None):
        super().__init__()
        self._url = url
        self._session = None
        self._ws_context = None
        self._ws = websocket
        self._ws_is_internal: bool = websocket is None
        self._message_handler = None
        self._connection_ready = asyncio.Event()

    async def connect(self):
        if self._ws_is_internal:
            self._session = aiohttp.ClientSession()
            self._ws_context = self._session.ws_connect(self._url)
            self._ws = await self._ws_context.__aenter__()

        self._connection_ready.set()
        self._message_handler = asyncio.create_task(self.handle_incoming_ws_messages())

    async def handle_incoming_ws_messages(self):
        await self._connection_ready.wait()
        try:
            async for msg in self._ws:
                if msg.type == aiohttp.WSMsgType.BINARY:
                    async for frame in self._frame_parser.receive_data(msg.data, 0):
                        self._incoming_frame_queue.put_nowait(frame)
        except asyncio.CancelledError:
            logger().debug('Asyncio task canceled: incoming_data_listener')
        except Exception:
            self._incoming_frame_queue.put_nowait(RSocketTransportError())

    async def send_frame(self, frame: Frame):
        await self._connection_ready.wait()
        with wrap_transport_exception():
            await self._ws.send_bytes(frame.serialize())

    async def close(self):
        if self._ws_is_internal:
            await self._ws_context.__aexit__(None, None, None)
            await self._session.__aexit__(None, None, None)

        self._message_handler.cancel()
        await self._message_handler


class TransportAioHttpWebsocket(AbstractMessagingTransport):
    def __init__(self, websocket):
        super().__init__()
        self._ws = websocket

    async def _message_generator(self):
        with wrap_transport_exception():
            async for msg in self._ws:
                if msg.type == aiohttp.WSMsgType.BINARY:
                    yield msg.data

    async def handle_incoming_ws_messages(self):
        try:
            async for message in self._message_generator():
                async for frame in self._frame_parser.receive_data(message, 0):
                    self._incoming_frame_queue.put_nowait(frame)
        except asyncio.CancelledError:
            logger().debug('Asyncio task canceled: aiohttp_handle_incoming_ws_messages')

    async def send_frame(self, frame: Frame):
        with wrap_transport_exception():
            await self._ws.send_bytes(frame.serialize())

    async def close(self):
        await self._ws.close()
