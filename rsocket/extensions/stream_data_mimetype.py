from typing import Optional

from rsocket.extensions.composite_metadata import CompositeMetadataItem, serialize_well_known_encoding, \
    parse_well_known_encoding
from rsocket.extensions.mimetypes import WellKnownMimeTypes


class StreamDataMimetype(CompositeMetadataItem):
    __slots__ = (
        'data_encoding'
    )

    def __init__(self, data_encoding: Optional[bytes] = None):
        super().__init__(WellKnownMimeTypes.MESSAGE_RSOCKET_MIMETYPE.value.name, None)
        self.data_encoding = data_encoding

    def parse(self, buffer: bytes):
        self.data_encoding, _ = parse_well_known_encoding(buffer, WellKnownMimeTypes.require_by_id)

    def serialize(self) -> bytes:
        return serialize_well_known_encoding(self.data_encoding, WellKnownMimeTypes.get_by_name)
