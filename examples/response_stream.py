from datetime import timedelta
from typing import AsyncGenerator, Tuple, Optional

from rsocket.payload import Payload
from rsocket.streams.stream_from_generator import StreamFromGenerator


class ResponseStream(StreamFromGenerator):

    def __init__(self,
                 response_count: int = 3,
                 delay_between_messages=timedelta(0),
                 fragment_size: Optional[int] = None):
        super().__init__(delay_between_messages, fragment_size)

        self._response_count = response_count
        self._current_response = 0

    async def generate_next_n(self, n: int) -> AsyncGenerator[Tuple[Payload, bool], None]:
        for i in range(n):
            is_complete = (self._current_response + 1) == self._response_count

            if self._delay_between_messages.total_seconds() > 0:
                message = 'Slow Item'
            else:
                message = 'Item'

            message = '%s: %s' % (message, self._current_response)
            yield Payload(message.encode('utf-8'), b'metadata'), is_complete

            if is_complete:
                break

            self._current_response += 1
