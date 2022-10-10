import abc
import asyncio

from rsocket.transports.transport import Transport


class AbstractMessagingTransport(Transport, metaclass=abc.ABCMeta):
    def __init__(self):
        super().__init__()
        self._incoming_frame_queue = asyncio.Queue()

    async def next_frame_generator(self):
        frame = await self._incoming_frame_queue.get()

        if isinstance(frame, Exception):
            raise frame

        async def frame_generator():
            yield frame

        return frame_generator()
