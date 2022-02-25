from typing import Union, Optional

ByteTypes = Union[bytes, bytearray]


class Payload:
    __slots__ = ('data', 'metadata')

    @staticmethod
    def _check(obj):
        assert obj is None or isinstance(obj, (bytes, bytearray))

    def __init__(self, data: Optional[ByteTypes] = None, metadata: Optional[ByteTypes] = None):
        self._check(data)
        self._check(metadata)

        self.data = ensure_bytes(data)
        self.metadata = ensure_bytes(metadata)

    def __str__(self):
        return "<payload: {}, {}>".format(self.data, self.metadata)

    def __eq__(self, other):
        return self.data == other.data and self.metadata == other.metadata

    def __repr__(self):
        return "Payload({}, {})".format(self.data, self.metadata)


def ensure_bytes(data: Optional[ByteTypes]) -> Optional[bytes]:
    if data is None:
        return data

    if isinstance(data, bytes):
        return data

    return bytes(data)
