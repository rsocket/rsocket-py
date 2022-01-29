import asyncio

from quart import websocket

from rsocket.frame import Frame
from rsocket.rsocket_server import RSocketServer
from rsocket.transports.transport import Transport


async def websocket_handler(*args, **kwargs):
    transport = TransportQuartWebsocket()
    RSocketServer(transport=transport, *args, **kwargs)
    await transport.handle_incoming_ws_messages()


class TransportQuartWebsocket(Transport):
    def __init__(self):
        super().__init__()
        self._incoming_frame_queue = asyncio.Queue()

    async def handle_incoming_ws_messages(self):
        try:
            while True:
                data = await websocket.receive()

                async for frame in self._frame_parser.receive_data(data, 0):
                    self._incoming_frame_queue.put_nowait(frame)
        except asyncio.CancelledError:
            pass

    async def send_frame(self, frame: Frame):
        await websocket.send(frame.serialize().decode('utf-8'))

    async def next_frame_generator(self, is_server_alive):
        frame = await self._incoming_frame_queue.get()

        async def frame_generator():
            yield frame

        return frame_generator()

    async def close(self):
        pass
