from typing import AsyncGenerator, Tuple

from rsocket.payload import Payload
from rsocket.streams.exceptions import FinishedIterator
from rsocket.streams.stream_from_generator import StreamFromGenerator


class StreamFromAsyncGenerator(StreamFromGenerator):
    async def _start_generator(self):
        self._iteration = self._generator().__aiter__()

    async def _generate_next_n(self, n: int) -> AsyncGenerator[Tuple[Payload, bool], None]:
        is_complete_sent = False
        for i in range(n):
            try:
                next_value = await self._iteration.__anext__()
                is_complete_sent = next_value[1]
                yield next_value
            except StopAsyncIteration:
                if not is_complete_sent:
                    raise FinishedIterator()
                return
