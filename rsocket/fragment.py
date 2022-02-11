from typing import Optional

from rsocket.payload import Payload


class Fragment(Payload):
    __slots__ = 'is_last'

    def __init__(self, data: Optional[bytes] = None, metadata: Optional[bytes] = None, is_last: bool = True):
        super().__init__(data, metadata)
        self.is_last = is_last
