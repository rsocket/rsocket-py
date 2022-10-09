from asyncio import StreamReader, StreamWriter

from rsocket.frame import Frame, serialize_with_frame_size_header
from rsocket.helpers import wrap_transport_exception
from rsocket.transports.transport import Transport


class TransportTCP(Transport):
    def __init__(self, reader: StreamReader, writer: StreamWriter):
        super().__init__()
        self._writer = writer
        self._reader = reader

    async def send_frame(self, frame: Frame):
        with wrap_transport_exception():
            self._writer.write(serialize_with_frame_size_header(frame))

    async def on_send_queue_empty(self):
        with wrap_transport_exception():
            await self._writer.drain()

    async def close(self):
        self._writer.close()
        await self._writer.wait_closed()

    async def next_frame_generator(self):
        with wrap_transport_exception():
            data = await self._reader.read(1024)

            if not data:
                self._writer.close()
                return

        return self._frame_parser.receive_data(data)

    def requires_length_header(self) -> bool:
        return True
