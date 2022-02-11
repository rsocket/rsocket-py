from asyncio import StreamReader, StreamWriter

from rsocket.frame import Frame, serialize_with_frame_size_header
from rsocket.logger import logger
from rsocket.transports.transport import Transport


class TransportTCP(Transport):
    def __init__(self, reader: StreamReader, writer: StreamWriter):
        super().__init__()
        self._writer = writer
        self._reader = reader

    async def send_frame(self, frame: Frame):
        self._writer.write(serialize_with_frame_size_header(frame))

    async def on_send_queue_empty(self):
        await self._writer.drain()

    async def close(self):
        self._writer.close()
        await self._writer.wait_closed()

    async def next_frame_generator(self, is_server_alive):
        try:
            data = await self._reader.read(1024)
        except (ConnectionResetError, BrokenPipeError) as exception:
            logger().debug(str(exception))
            return  # todo: workaround to silence errors on client closing. this needs a better solution.

        if not data:
            self._writer.close()
            return

        return self._frame_parser.receive_data(data)
