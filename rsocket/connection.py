import struct

from rsocket.frame import parse

__all__ = ['Connection']


class Connection:
    def __init__(self):
        self._buffer = bytearray()

    def receive_data(self, data: bytes):
        self._buffer.extend(data)
        total = len(self._buffer)
        events = []
        offset = 0
        while offset + 4 <= total:
            length, = struct.unpack_from('>I', self._buffer, offset)
            if len(self._buffer) < length:
                break
            events.append(parse(self._buffer[:length], offset))
            self._buffer = self._buffer[length:]
            total -= length
        return events
