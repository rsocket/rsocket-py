from typing import Optional

from rsocket.frame_helpers import safe_len
from rsocket.local_typing import ByteTypes


class Payload:
    """
    A response/stream message (upstream or downstream). Contains data and metadata, both `bytes`.

    :param data: data segment of payload
    :param metadata: metadata segment of payload
    """

    __slots__ = ('data', 'metadata')

    def __init__(self, data: Optional[ByteTypes] = None, metadata: Optional[ByteTypes] = None):
        self.data = data
        self.metadata = metadata

    def __str__(self):
        return f"<payload: data_length {safe_len(self.data)}, metadata_length {safe_len(self.metadata)}>"

    def __eq__(self, other):
        return self.data == other.data and self.metadata == other.metadata

    def __repr__(self):
        return f"Payload({repr(self.data)}, {repr(self.metadata)})"
