from typing import Optional

from rsocket.extensions.composite_metadata import CompositeMetadataItem, serialize_metadata_encoding, \
    parse_item_metadata_encoding
from rsocket.extensions.mimetypes import WellKnownMimeTypes


class StreamDataMimetype(CompositeMetadataItem):
    __slots__ = (
        'data_encoding'
    )

    def __init__(self, data_encoding: Optional[bytes] = None):
        super().__init__(WellKnownMimeTypes.MESSAGE_RSOCKET_MIMETYPE.value.name, None)
        self.data_encoding = data_encoding

    def parse(self, buffer: bytes):
        self.data_encoding, _ = parse_item_metadata_encoding(buffer)

    def serialize(self) -> bytes:
        return serialize_metadata_encoding(self.data_encoding)
