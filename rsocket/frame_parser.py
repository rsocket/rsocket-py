import struct
from typing import AsyncGenerator

from rsocket import frame

__all__ = ['FrameParser']

from rsocket.frame import Frame


class FrameParser:
    def __init__(self):
        self._buffer = bytearray()

    async def receive_data(self, data: bytes) -> AsyncGenerator[Frame, None]:
        self._buffer.extend(data)
        total = len(self._buffer)

        frame_length_byte_count = 3

        while total >= frame_length_byte_count:
            length, = struct.unpack('>I', b'\x00' + self._buffer[:frame_length_byte_count])

            if total < length + frame_length_byte_count:
                return

            yield frame.parse(self._buffer[:length + frame_length_byte_count])

            self._buffer = self._buffer[length + frame_length_byte_count:]
            total -= length + frame_length_byte_count