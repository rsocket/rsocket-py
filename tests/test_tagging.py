import struct
import unittest

from rsocket.exceptions import RSocketError
from rsocket.extensions.tagging import TaggingMetadata
from rsocket.extensions.mimetypes import WellKnownMimeTypes


class TestTaggingMetadata(unittest.TestCase):
    def test_serialize_max_length(self):
        tag = 's' * 255
        meta = TaggingMetadata(WellKnownMimeTypes.MESSAGE_RSOCKET_ROUTING, [tag])
        serialized = meta.serialize()
        length = struct.pack('>B', len(tag))
        self.assertEquals(length + bytes(tag, 'utf-8'), serialized)

    def test_serialize_exception_length(self):
        tag = 's' * 256
        meta = TaggingMetadata(WellKnownMimeTypes.MESSAGE_RSOCKET_ROUTING, [tag])
        with self.assertRaisesRegex(RSocketError, f'Tag length longer than 255 characters: "b\'{tag}\'"'):
            meta.serialize()

    def test_parse(self):
        meta = TaggingMetadata(WellKnownMimeTypes.MESSAGE_RSOCKET_ROUTING)

        tag = 's' * 255
        length = struct.pack('>B', len(tag))

        meta.parse(length + bytes(tag, 'utf-8'))

        self.assertEquals(tag, meta.tags[0].decode())
