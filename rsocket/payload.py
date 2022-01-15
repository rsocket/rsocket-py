import json
from typing import Any, Callable


class Payload:
    __slots__ = ('data', 'metadata')

    @staticmethod
    def _check(obj):
        assert obj is None or isinstance(obj, (bytes, bytearray))

    def __init__(self, data: bytes = None, metadata: bytes = None):
        self._check(data)
        self._check(metadata)

        self.data = data
        self.metadata = metadata

    def __str__(self):
        data, metadata = self.data.decode(), self.metadata.decode()
        return "<payload: '{}', '{}'>".format(data, metadata)

    def __eq__(self, other):
        return self.data == other.data and self.metadata == other.metadata


def _default_serializer(obj: Any) -> bytes:
    return bytes(json.dumps(obj))


def payload_from_any(data: Any,
                     metadata: Any,
                     serializer: Callable[[Any], bytes] = _default_serializer) -> Payload:
    return Payload(serializer(data), serializer(metadata))
