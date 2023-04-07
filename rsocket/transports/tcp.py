from asyncio import StreamReader, StreamWriter

from rsocket.frame import Frame, serialize_prefix_with_frame_size_header
from rsocket.helpers import wrap_transport_exception
from rsocket.transports.transport import Transport


class TransportTCP(Transport):
    """
    RSocket transport over asyncio TCP connection.

    :param reader: asyncio connection reader stream
    :param writer: asyncio connection writer stream
    """

    def __init__(self,
                 reader: StreamReader,
                 writer: StreamWriter,
                 read_buffer_size=1024):
        super().__init__()
        self._read_buffer_size = read_buffer_size
        self._writer = writer
        self._reader = reader

    async def send_frame(self, frame: Frame):
        await self.serialize_partial(frame)

    async def serialize_partial(self, frame: Frame):
        with wrap_transport_exception():
            self._writer.write(serialize_prefix_with_frame_size_header(frame))
            frame.write_data_metadata(self._writer.write)
            await self._writer.drain()

    async def on_send_queue_empty(self):
        with wrap_transport_exception():
            await self._writer.drain()

    async def close(self):
        self._writer.close()
        await self._writer.wait_closed()

    async def next_frame_generator(self):
        with wrap_transport_exception():
            data = await self._reader.read(self._read_buffer_size)

            if not data:
                self._writer.close()
                return

        return self._frame_parser.receive_data(data)

    def requires_length_header(self) -> bool:
        return True
