import asyncio
from contextlib import asynccontextmanager

import aiohttp
from aiohttp import web

from rsocket.frame import Frame
from rsocket.frame_parser import FrameParser
from rsocket.rsocket_client import RSocketClient
from rsocket.rsocket_server import RSocketServer
from rsocket.transports.transport import Transport


@asynccontextmanager
async def websocket_client(url, *args, **kwargs) -> RSocketClient:
    session = aiohttp.ClientSession()

    async with session.ws_connect(url) as ws:
        transport = TransportWebsocket(ws)
        asyncio.create_task(transport.handle_incoming_ws_messages())
        async with RSocketClient(transport, *args, **kwargs) as client:
            yield client


def websocket_handler_factory(*args, **kwargs):
    async def websocket_handler(request):
        ws = web.WebSocketResponse()
        await ws.prepare(request)
        transport = TransportWebsocket(ws)
        RSocketServer(transport, *args, **kwargs)
        await transport.handle_incoming_ws_messages()
        return ws

    return websocket_handler


class TransportWebsocket(Transport):
    def __init__(self, websocket):
        self._frame_parser = FrameParser()
        self._incoming_frame_queue = asyncio.Queue()
        self._ws = websocket

    async def handle_incoming_ws_messages(self):
        async for msg in self._ws:
            if msg.type == aiohttp.WSMsgType.BINARY:
                async for frame in self._frame_parser.receive_data(msg.data, 0):
                    self._incoming_frame_queue.put_nowait(frame)

    async def on_send_queue_empty(self):
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

    async def close_writer(self):
        await self._ws.close()
