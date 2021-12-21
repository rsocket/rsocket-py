import struct
from typing import Union, List

from rsocket.extensions.mimetypes import WellKnownMimeTypes

_default = object()


def _default_or_value(value, default=None):
    if value == _default:
        return default
    return value


class CompositeMetadataItem:
    __slots__ = (
        'metadata_encoding',
        'metadata'
    )

    def __init__(self,
                 metadata_type: Union[str, WellKnownMimeTypes] = _default,
                 body: bytes = _default):
        self.metadata_encoding = _default_or_value(metadata_type)
        self.metadata = _default_or_value(body)

    def parse(self, buffer: bytes, offset: int):
        is_known_mime_id = struct.unpack('>B', buffer[offset:offset + 1])[0] >> 7 == 1
        mime_length_or_type = (struct.unpack('>B', buffer[offset:offset + 1])[0]) & 0b1111111
        offset += 1

        if is_known_mime_id:
            self.metadata_encoding = WellKnownMimeTypes.require_by_id(mime_length_or_type).value[0]
        else:
            self.metadata_encoding = buffer[offset:offset + mime_length_or_type]
            offset += mime_length_or_type

        length, = struct.unpack('>I', b'\x00' + buffer[offset:offset + 3])
        offset += 3
        self.metadata = buffer[offset:offset + length]
        offset += length
        return offset

    def serialize(self, middle=b'') -> bytes:
        ...


class CompositeMetadata:
    __slots__ = (
        'items'
    )

    def __init__(self, items: List[CompositeMetadataItem] = _default):
        self.items = _default_or_value(items, [])

    def parse(self, buffer: bytes, offset: int):
        item = CompositeMetadataItem()
        item.parse(buffer, offset)
        self.items.append(item)

    def serialize(self, middle=b'') -> bytes:
        ...
