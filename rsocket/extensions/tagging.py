import struct
from typing import Union, List, Optional

from rsocket.exceptions import RSocketError
from rsocket.extensions.composite_metadata_item import CompositeMetadataItem
from rsocket.frame_helpers import ensure_bytes


class TaggingMetadata(CompositeMetadataItem):
    __slots__ = (
        'tags'
    )

    def __init__(self, encoding: bytes, tags: Optional[List[Union[bytes, str]]] = None):
        self.tags = tags
        self.encoding = encoding

        super().__init__(encoding, None)

    def serialize(self) -> bytes:
        self.content = self._serialize_tags()
        return super().serialize()

    def _serialize_tags(self) -> bytes:
        serialized = b''

        for tag in list(map(ensure_bytes, self.tags)):
            if len(tag) > 256:
                raise RSocketError('Tag length longer than 256 characters: "%s"' % tag)

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

    def __eq__(self, other):
        if isinstance(other, self.__class__):
            return self.tags == other.tags and self.encoding == other.encoding

        return False
