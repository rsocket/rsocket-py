import asyncio
from datetime import timedelta
from io import BytesIO
from typing import AsyncGenerator, Tuple, Any, Optional

from rsocket.fragment import Fragment
from rsocket.helpers import payload_to_n_size_fragments
from rsocket.response_stream_from_generator import ResponseStreamFromGenerator


class ResponseStream(ResponseStreamFromGenerator):

    def __init__(self,
                 response_count: int = 3,
                 delay_between_messages=timedelta(0),
                 fragment_size: Optional[int] = None):
        super().__init__()
        self._fragment_size = fragment_size
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

            message = ('%s: %s' % (message, self._current_response)).encode('utf-8')
            metadata = b'metadata'

            if self._fragment_size is None:
                yield Fragment(message, metadata), is_complete
            else:
                async for fragment in payload_to_n_size_fragments(BytesIO(message), BytesIO(metadata), 6):
                    yield fragment, is_complete and fragment.is_last

            await asyncio.sleep(self._delay_between_messages.total_seconds())

            if is_complete:
                break

            self._current_response += 1
