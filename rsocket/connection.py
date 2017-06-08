import struct

from rsocket import frame

__all__ = ['Connection']


class Connection:
    def __init__(self):
        self._buffer = bytearray()

    def receive_data(self, data: bytes):
        self._buffer.extend(data)
        total = len(self._buffer)
        events = []

        while total >= 3:
            length, = struct.unpack('>I', b'\x00' + self._buffer[:3])
            if total < length + 3:
                break
            events.append(frame.parse(self._buffer[:length + 3]))
            self._buffer = self._buffer[length + 3:]
            total -= length + 3
        return events
