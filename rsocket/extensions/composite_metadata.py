from typing import List, Type, Union

from rsocket.extensions.authentication_content import AuthenticationContent
from rsocket.extensions.composite_metadata_item import CompositeMetadataItem
from rsocket.extensions.mimetypes import WellKnownMimeTypes, ensure_encoding_name
from rsocket.extensions.routing import RoutingMetadata
from rsocket.extensions.stream_data_mimetype import StreamDataMimetype
from rsocket.extensions.stream_data_mimetype import StreamDataMimetypes
from rsocket.frame_helpers import (pack_24bit_length, unpack_24bit)
from rsocket.helpers import parse_well_known_encoding, serialize_well_known_encoding

_default = object()


def default_or_value(value, default=None):
    if value is _default:
        return default
    return value


def metadata_item_factory(metadata_encoding: bytes) -> Type[CompositeMetadataItem]:
    metadata_item_factory_by_type = {
        WellKnownMimeTypes.MESSAGE_RSOCKET_ROUTING.value.name: RoutingMetadata,
        WellKnownMimeTypes.MESSAGE_RSOCKET_MIMETYPE.value.name: StreamDataMimetype,
        WellKnownMimeTypes.MESSAGE_RSOCKET_ACCEPT_MIMETYPES.value.name: StreamDataMimetypes,
        WellKnownMimeTypes.MESSAGE_RSOCKET_AUTHENTICATION.value.name: AuthenticationContent
    }

    return metadata_item_factory_by_type.get(metadata_encoding, CompositeMetadataItem)


class CompositeMetadata:
    __slots__ = 'items'

    def __init__(self, items: List[CompositeMetadataItem] = _default):

        self.items: List[CompositeMetadataItem] = default_or_value(items, [])

    def append(self, item: CompositeMetadataItem) -> 'CompositeMetadata':
        self.items.append(item)
        return self

    def find_by_mimetype(self, mimetype: Union[WellKnownMimeTypes, str, bytes]) -> List[CompositeMetadataItem]:
        mimetype_name = ensure_encoding_name(mimetype)
        return [item for item in self.items if ensure_encoding_name(item.encoding) == mimetype_name]

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

            length = unpack_24bit(metadata, offset)
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
            item_serialized += pack_24bit_length(item_metadata)
            item_serialized += item_metadata

            serialized += item_serialized

        return serialized
