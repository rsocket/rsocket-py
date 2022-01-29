import asyncio
from contextlib import asynccontextmanager

import aiohttp
from aiohttp import web

from rsocket.frame import Frame
from rsocket.rsocket_client import RSocketClient
from rsocket.rsocket_server import RSocketServer
from rsocket.transports.transport import Transport


@asynccontextmanager
async def websocket_client(url, *args, **kwargs) -> RSocketClient:
    async with aiohttp.ClientSession() as session:
        async with session.ws_connect(url) as ws:
            transport = TransportWebsocket(ws)
            message_handler = asyncio.create_task(transport.handle_incoming_ws_messages())
            async with RSocketClient(transport, *args, **kwargs) as client:
                yield client

            message_handler.cancel()
            await message_handler


def websocket_handler_factory(*args, on_server_create=None, **kwargs):
    async def websocket_handler(request):
        ws = web.WebSocketResponse()
        await ws.prepare(request)
        transport = TransportWebsocket(ws)
        server = RSocketServer(transport, *args, **kwargs)

        if on_server_create is not None:
            on_server_create(server)

        await transport.handle_incoming_ws_messages()
        return ws

    return websocket_handler


class TransportWebsocket(Transport):
    def __init__(self, websocket):
        super().__init__()
        self._incoming_frame_queue = asyncio.Queue()
        self._ws = websocket

    async def handle_incoming_ws_messages(self):
        try:
            async for msg in self._ws:
                if msg.type == aiohttp.WSMsgType.BINARY:
                    async for frame in self._frame_parser.receive_data(msg.data, 0):
                        self._incoming_frame_queue.put_nowait(frame)
        except asyncio.CancelledError:
            pass

    async def send_frame(self, frame: Frame):
        await self._ws.send_bytes(frame.serialize())

    async def next_frame_generator(self, is_server_alive):
        frame = await self._incoming_frame_queue.get()

        async def frame_generator():
            yield frame

        return frame_generator()

    async def close(self):
        await self._ws.close()
