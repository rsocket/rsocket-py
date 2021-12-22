import struct
from typing import Union, List, Optional

from rsocket.extensions.composite_metadata import CompositeMetadataItem


class TaggingMetadata(CompositeMetadataItem):
    __slots__ = (
        'tags',
        'encoding'
    )

    def __init__(self, encoding:bytes, tags: Optional[List[Union[bytes, str]]] = None):
        self.tags = tags
        self.encoding = encoding

        super().__init__(encoding, None)

    def serialize(self) -> bytes:
        self.content = self._serialize_tags()
        return super().serialize()

    def _serialize_tags(self) -> bytes:
        serialized = b''

        for tag in list(map(self._ensure_bytes, self.tags)):
            if len(tag) > 256:
                raise Exception('Tag length longer than 256 characters')

            serialized += struct.pack('>b', len(tag))
            serialized += tag

        return serialized

    def parse(self, buffer: bytes):
        self.tags = []
        offset = 0

        while offset < len(buffer):
            tag_length = struct.unpack('>b', buffer[offset:offset + 1])[0]
            offset += 1
            self.tags.append(buffer[offset:offset + tag_length])
            offset += tag_length
