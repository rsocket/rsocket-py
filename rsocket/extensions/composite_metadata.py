import struct
from typing import Union, List, Optional, Type, Tuple

from rsocket.extensions.mimetypes import WellKnownMimeTypes

_default = object()


def default_or_value(value, default=None):
    if value == _default:
        return default
    return value


class CompositeMetadataItem:
    __slots__ = (
        'encoding',
        'content'
    )

    def __init__(self,
                 encoding: Union[bytes, WellKnownMimeTypes] = _default,
                 body: Optional[bytes] = _default):
        self.encoding = default_or_value(encoding)
        self.content = default_or_value(body)

    def parse(self, buffer: bytes):
        self.content = buffer

    def serialize(self) -> bytes:
        return self.content

    def _str_to_bytes(self, route_path):
        return bytes(bytearray(map(ord, route_path)))

    def _ensure_bytes(self, tag: Union[bytes, str]) -> bytes:
        if isinstance(tag, str):
            return self._str_to_bytes(tag)
        return tag


def metadata_item_factory(metadata_encoding: bytes) -> Type[CompositeMetadataItem]:
    from rsocket.extensions.routing import RoutingMetadata
    from rsocket.extensions.stream_data_mimetype import StreamDataMimetype

    metadata_item_factory_by_type = {
        WellKnownMimeTypes.MESSAGE_RSOCKET_ROUTING.value.name: RoutingMetadata,
        WellKnownMimeTypes.MESSAGE_RSOCKET_MIMETYPE.value.name: StreamDataMimetype
    }

    return metadata_item_factory_by_type.get(metadata_encoding, CompositeMetadataItem)


def serialize_metadata_encoding(encoding: bytes) -> bytes:
    metadata_known_type = WellKnownMimeTypes.get_by_name(encoding)

    if metadata_known_type is None:
        metadata_encoding_length = len(encoding)

        if metadata_encoding_length > 0b1111111:
            raise Exception('metadata encoding type too long')

        serialized = ((0 << 7) | metadata_encoding_length & 0b1111111).to_bytes(1, 'big')
        serialized += encoding
    else:
        serialized = ((1 << 7) | metadata_known_type.id & 0b1111111).to_bytes(1, 'big')

    return serialized


def parse_item_metadata_encoding(buffer) -> Tuple[bytes, int]:
    is_known_mime_id = struct.unpack('>B', buffer[:1])[0] >> 7 == 1
    mime_length_or_type = (struct.unpack('>B', buffer[:1])[0]) & 0b1111111
    if is_known_mime_id:
        metadata_encoding = WellKnownMimeTypes.require_by_id(mime_length_or_type).name
        offset = 1
    else:
        metadata_encoding = buffer[1:1 + mime_length_or_type]
        offset = 1 + mime_length_or_type

    return metadata_encoding, offset


class CompositeMetadata:
    __slots__ = (
        'items',
        'metadata_item_factory_by_type'
    )

    def __init__(self, items: List[CompositeMetadataItem] = _default):

        self.items: List[CompositeMetadataItem] = default_or_value(items, [])

    def append(self, item: CompositeMetadataItem) -> 'CompositeMetadata':
        self.items.append(item)
        return self

    def parse(self, metadata: bytes):
        composite_length = len(metadata)
        offset = 0

        while offset < composite_length:
            metadata_encoding, relative_offset = parse_item_metadata_encoding(metadata[offset:])
            offset += relative_offset

            length, = struct.unpack('>I', b'\x00' + metadata[offset:offset + 3])
            offset += 3

            item = metadata_item_factory(metadata_encoding)()
            item.encoding = metadata_encoding

            item_metadata = metadata[offset:offset + length]
            metadata_length = len(item_metadata)
            item.parse(item_metadata)

            self.append(item)
            offset += metadata_length

    def serialize(self) -> bytes:
        serialized = b''

        for item in self.items:
            serialized += serialize_metadata_encoding(item.encoding)
            item_metadata = item.serialize()
            serialized += struct.pack('>I', len(item_metadata))[1:]
            serialized += item_metadata

        return serialized
