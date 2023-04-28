import struct
from typing import AsyncGenerator

from rsocket.logger import logger

__all__ = ['FrameParser']

from rsocket.frame import Frame, InvalidFrame, parse_or_ignore


class FrameParser:
    def __init__(self):
        self._buffer = bytearray()

    async def receive_data(self, data: bytes, header_length=3) -> AsyncGenerator[Frame, None]:
        self._buffer.extend(data)
        total = len(self._buffer)
        frame_length_byte_count = header_length

        while total >= frame_length_byte_count:
            buffer_view = memoryview(self._buffer)

            if header_length > 0:
                length = struct.unpack('>I', b'\x00' + buffer_view[:frame_length_byte_count])[0]
            else:
                length = len(data)

            if total < length + frame_length_byte_count:
                del buffer_view
                return

            try:
                new_frame = parse_or_ignore(buffer_view[frame_length_byte_count:length + frame_length_byte_count])

                if new_frame is not None:
                    yield new_frame
            except Exception:
                logger().error('Error parsing frame', exc_info=True)
                yield InvalidFrame()

            del buffer_view
            self._buffer = self._buffer[length + frame_length_byte_count:]
            total -= length + frame_length_byte_count
