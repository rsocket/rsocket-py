from typing import AsyncGenerator, Tuple, Any

from rsocket.queue_response_stream import QueueResponseStream


class ResponseStream(QueueResponseStream):

    def __init__(self, response_count: int = 3):
        super().__init__()
        self._response_count = response_count
        self._current_response = 0

    async def generate_next_n(self, n: int) -> AsyncGenerator[Tuple[Any, bool], None]:
        for i in range(n):
            is_complete = (self._current_response + 1) == self._response_count

            yield ("Item: %s" % self._current_response), is_complete

            if is_complete:
                break

            self._current_response += 1
