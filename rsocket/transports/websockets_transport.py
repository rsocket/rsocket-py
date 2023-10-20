import asyncio
from asyncio import Queue

from rsocket.frame import Frame
from rsocket.transports.abstract_messaging import AbstractMessagingTransport


class WebsocketsTransport(AbstractMessagingTransport):

    def __init__(self):
        super().__init__()
        self._outgoing_frame_queue = Queue()

    async def send_frame(self, frame: Frame):
        await self._outgoing_frame_queue.put(frame)

    async def close(self):
        pass

    async def consumer_handler(self, websocket):
        async for message in websocket:
            async for frame in self._frame_parser.receive_data(message, header_length=0):
                await self._incoming_frame_queue.put(frame)

    async def producer_handler(self, websocket):
        while True:
            frame = await self._outgoing_frame_queue.get()
            await websocket.send(frame.serialize())

    async def handler(self, websocket):
        consumer_task = asyncio.create_task(self.consumer_handler(websocket))
        producer_task = asyncio.create_task(self.producer_handler(websocket))

        done, pending = await asyncio.wait(
            [consumer_task, producer_task],
            return_when=asyncio.FIRST_COMPLETED,
        )

        for task in pending:
            task.cancel()
