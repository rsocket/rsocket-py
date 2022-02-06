from typing import AsyncGenerator, Tuple

from rsocket.payload import Payload
from rsocket.streams.stream_from_generator import StreamFromGenerator


class StreamFromAsyncGenerator(StreamFromGenerator):
    async def _start_generator(self):
        self._iteration = self._generator().__aiter__()

    async def _generate_next_n(self, n: int) -> AsyncGenerator[Tuple[Payload, bool], None]:
        for i in range(n):
            try:
                item = await self._iteration.__anext__()
            except StopAsyncIteration:
                return
            if item != self._iteration:
                yield item
            else:
                return
