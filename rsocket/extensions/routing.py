import struct
from typing import Union, List, Optional

from rsocket.extensions.composite_metadata import CompositeMetadataItem
from rsocket.extensions.mimetypes import WellKnownMimeTypes


class RoutingMetadata(CompositeMetadataItem):
    __slots__ = (
        'tags',
    )

    def __init__(self, tags: Optional[List[Union[bytes, str]]] = None):
        self.tags = tags

        super().__init__(WellKnownMimeTypes.MESSAGE_RSOCKET_ROUTING.value[0], None)

    def serialize(self) -> bytes:
        self.metadata = self._serialize_tags()
        return super().serialize()

    def _serialize_tags(self) -> bytes:
        serialized = b''

        for tag in list(map(self._ensure_bytes, self.tags)):
            if len(tag) > 256:
                raise Exception('Routing tag length longer than 256 characters')

            serialized += struct.pack('>b', len(tag))
            serialized += tag

        return serialized

    def parse(self, buffer: bytes):
        self.tags = []
        offset = 0

        while offset < len(buffer):
            tag_length = struct.unpack('>b', buffer[offset:offset+1])[0]
            offset += 1
            self.tags.append(buffer[offset:offset + tag_length])
            offset += tag_length
