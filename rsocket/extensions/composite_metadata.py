import struct
from typing import Union, List, Optional, Type, Tuple

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
                 body: Optional[bytes] = _default):
        self.metadata_encoding = _default_or_value(metadata_type)
        self.metadata = _default_or_value(body)

    def parse(self, buffer: bytes):
        self.metadata = buffer

    def serialize(self) -> bytes:
        return self.metadata

    def _str_to_bytes(self, route_path):
        return bytes(bytearray(map(ord, route_path)))

    def _ensure_bytes(self, tag: Union[bytes, str]) -> bytes:
        if isinstance(tag, str):
            return self._str_to_bytes(tag)
        return tag


def metadata_item_factory(metadata_encoding: bytes) -> Type[CompositeMetadataItem]:
    from rsocket.extensions.routing import RoutingMetadata

    metadata_item_factory_by_type = {
        WellKnownMimeTypes.MESSAGE_RSOCKET_ROUTING.value[0]: RoutingMetadata
    }
    return metadata_item_factory_by_type.get(metadata_encoding, CompositeMetadataItem)


class CompositeMetadata:
    __slots__ = (
        'items',
        'metadata_item_factory_by_type'
    )

    def __init__(self, items: List[CompositeMetadataItem] = _default):

        self.items: List[CompositeMetadataItem] = _default_or_value(items, [])

    def append(self, item: CompositeMetadataItem) -> 'CompositeMetadata':
        self.items.append(item)
        return self

    def parse(self, metadata: bytes):
        composite_length = len(metadata)
        offset = 0

        while offset < composite_length:
            metadata_encoding, offset = self._parse_item_metadata_encoding(metadata[offset:])

            length, = struct.unpack('>I', b'\x00' + metadata[offset:offset + 3])
            offset += 3

            item = metadata_item_factory(metadata_encoding)()
            item.metadata_encoding = metadata_encoding

            item_metadata = metadata[offset:offset + length]
            metadata_length = len(item_metadata)
            item.parse(item_metadata)

            self.items.append(item)
            offset += metadata_length

    def _parse_item_metadata_encoding(self, buffer) -> Tuple[bytes, int]:
        is_known_mime_id = struct.unpack('>B', buffer[:1])[0] >> 7 == 1
        mime_length_or_type = (struct.unpack('>B', buffer[:1])[0]) & 0b1111111
        if is_known_mime_id:
            metadata_encoding = WellKnownMimeTypes.require_by_id(mime_length_or_type).value[0]
            offset = 1
        else:
            metadata_encoding = buffer[1:1 + mime_length_or_type]
            offset = 1 + mime_length_or_type

        return metadata_encoding, offset

    def _serialize_item_metadata_encoding(self, item: CompositeMetadataItem) -> bytes:
        metadata_known_type = WellKnownMimeTypes.get_by_name(item.metadata_encoding)

        if metadata_known_type is None:
            metadata_encoding_length = len(item.metadata_encoding)

            if metadata_encoding_length > 0b1111111:
                raise Exception('metadata encoding type too long')

            serialized = ((0 << 7) | metadata_encoding_length & 0b1111111).to_bytes(1, 'big')
            serialized += item.metadata_encoding
        else:
            serialized = ((1 << 7) | metadata_known_type.value[1] & 0b1111111).to_bytes(1, 'big')
        return serialized

    def serialize(self) -> bytes:
        serialized = b''

        for item in self.items:
            serialized += self._serialize_item_metadata_encoding(item)
            item_metadata = item.serialize()
            serialized += struct.pack('>I', len(item_metadata))[1:]
            serialized += item_metadata

        return serialized
