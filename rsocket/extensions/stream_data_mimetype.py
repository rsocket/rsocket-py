from typing import Optional, List, Union

from rsocket.extensions.composite_metadata_item import CompositeMetadataItem
from rsocket.extensions.mimetypes import WellKnownMimeTypes, WellKnownMimeType, ensure_well_known_encoding_enum_value
from rsocket.helpers import parse_well_known_encoding, serialize_well_known_encoding


class StreamDataMimetype(CompositeMetadataItem):
    __slots__ = (
        'data_encoding'
    )

    def __init__(self, data_encoding: Optional[Union[bytes, WellKnownMimeType, WellKnownMimeTypes]] = None):
        super().__init__(WellKnownMimeTypes.MESSAGE_RSOCKET_MIMETYPE.value.name, None)
        data_encoding = ensure_well_known_encoding_enum_value(data_encoding)

        self.data_encoding = data_encoding

    def parse(self, buffer: bytes):
        self.data_encoding, _ = parse_well_known_encoding(buffer, WellKnownMimeTypes.require_by_id)

    def serialize(self) -> bytes:
        return serialize_well_known_encoding(self.data_encoding, WellKnownMimeTypes.get_by_name)


class StreamDataMimetypes(CompositeMetadataItem):
    __slots__ = (
        'data_encodings'
    )

    def __init__(self, data_encodings: Optional[List[Union[bytes, WellKnownMimeTypes]]] = None):
        super().__init__(WellKnownMimeTypes.MESSAGE_RSOCKET_ACCEPT_MIMETYPES.value.name, None)

        if data_encodings is None:
            data_encodings = []

        self.data_encodings = [ensure_well_known_encoding_enum_value(data_encoding)
                               for data_encoding in data_encodings]

    def parse(self, buffer: bytes):
        offset = 0

        while offset < len(buffer):
            data_encoding, offset_diff = parse_well_known_encoding(buffer[offset:], WellKnownMimeTypes.require_by_id)
            offset += offset_diff
            self.data_encodings.append(data_encoding)

    def serialize(self) -> bytes:
        serialized = b''

        for data_encoding in self.data_encodings:
            serialized += serialize_well_known_encoding(data_encoding, WellKnownMimeTypes.get_by_name)

        return serialized

    def __eq__(self, other):
        if isinstance(other, self.__class__):
            return (self.content == other.content
                    and self.encoding == other.encoding
                    and self.data_encodings == other.data_encodings)

        return False
