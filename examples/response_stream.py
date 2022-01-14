import asyncio
from datetime import timedelta
from typing import AsyncGenerator, Tuple, Any

from rsocket.queue_response_stream import QueueResponseStream


class ResponseStream(QueueResponseStream):

    def __init__(self, response_count: int = 3, delay_between_messages=timedelta(0)):
        super().__init__()
        self._delay_between_messages = delay_between_messages
        self._response_count = response_count
        self._current_response = 0

    async def generate_next_n(self, n: int) -> AsyncGenerator[Tuple[Any, bool], None]:
        for i in range(n):
            is_complete = (self._current_response + 1) == self._response_count
            if self._delay_between_messages.total_seconds() > 0:
                message = 'Slow Item'
            else:
                message = 'Item'

            yield ('%s: %s' % (message, self._current_response)), is_complete

            await asyncio.sleep(self._delay_between_messages.total_seconds())

            if is_complete:
                break

            self._current_response += 1
