from asyncio import Future
from typing import Optional

from rsocket.payload import Payload


class Fragment(Payload):
    __slots__ = ('is_last', 'sent_future')

    def __init__(self,
                 data: Optional[bytes] = None,
                 metadata: Optional[bytes] = None,
                 is_last: Optional[bool] = True,
                 sent_future: Optional[Future] = None):
        super().__init__(data, metadata)
        self.is_last = is_last
        self.sent_future = sent_future
