import struct
from typing import Union, List, Optional, Type

from rsocket.extensions.mimetypes import WellKnownMimeTypes
from rsocket.helpers import parse_well_known_encoding, serialize_well_known_encoding

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


def metadata_item_factory(metadata_encoding: bytes) -> Type[CompositeMetadataItem]:
    from rsocket.extensions.routing import RoutingMetadata
    from rsocket.extensions.stream_data_mimetype import StreamDataMimetype
    from rsocket.extensions.authentication_content import AuthenticationContent

    metadata_item_factory_by_type = {
        WellKnownMimeTypes.MESSAGE_RSOCKET_ROUTING.value.name: RoutingMetadata,
        WellKnownMimeTypes.MESSAGE_RSOCKET_MIMETYPE.value.name: StreamDataMimetype,
        WellKnownMimeTypes.MESSAGE_RSOCKET_AUTHENTICATION.value.name: AuthenticationContent
    }

    return metadata_item_factory_by_type.get(metadata_encoding, CompositeMetadataItem)


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

    def extend(self, *items: CompositeMetadataItem) -> 'CompositeMetadata':
        self.items.extend(items)
        return self

    def parse(self, metadata: bytes):
        composite_length = len(metadata)
        offset = 0

        while offset < composite_length:
            metadata_encoding, relative_offset = parse_well_known_encoding(metadata[offset:],
                                                                           WellKnownMimeTypes.require_by_id)
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
            metadata_header = serialize_well_known_encoding(item.encoding, WellKnownMimeTypes.get_by_name)
            item_metadata = item.serialize()

            item_serialized = b''
            item_serialized += metadata_header
            item_serialized += struct.pack('>I', len(item_metadata))[1:]
            item_serialized += item_metadata

            serialized += item_serialized

        return serialized
