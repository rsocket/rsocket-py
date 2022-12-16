import asyncio
from typing import AsyncGenerator, Tuple

from rsocket.async_helpers import async_range
from rsocket.payload import Payload
from rsocket.streams.exceptions import FinishedIterator
from rsocket.streams.stream_from_generator import StreamFromGenerator


class StreamFromAsyncGenerator(StreamFromGenerator):
    async def _start_generator(self):
        self._generator: AsyncGenerator = self._generator_factory()
        self._iteration = self._generator.__aiter__()

    def dispose(self):
        asyncio.create_task(self._generator.aclose())

    async def _generate_next_n(self, n: int) -> AsyncGenerator[Tuple[Payload, bool], None]:
        is_complete_sent = False
        async for i in async_range(n):
            try:
                next_value = await self._iteration.__anext__()
                is_complete_sent = next_value[1]
                yield next_value
            except StopAsyncIteration:
                if not is_complete_sent:
                    raise FinishedIterator()
                return
