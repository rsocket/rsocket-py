import logging
from typing import AsyncGenerator, Tuple

from reactivestreams.subscriber import Subscriber
from reactivestreams.subscription import Subscription
from rsocket.fragment import Fragment
from rsocket.payload import Payload
from rsocket.response_stream_from_generator import ResponseStreamFromGenerator


class ResponseChannel(ResponseStreamFromGenerator, Subscriber):
    def __init__(self, response_count: int = 3):
        super().__init__()
        self._response_count = response_count
        self._current_response = 0

    async def generate_next_n(self, n: int) -> AsyncGenerator[Tuple[Fragment, bool], None]:
        for i in range(n):
            is_complete = (self._current_response + 1) == self._response_count

            message = 'Item on channel: %s' % self._current_response
            yield Fragment(message.encode('utf-8'), b''), is_complete

            await self.subscription.request(1)
            if is_complete:
                break

            self._current_response += 1

    def on_subscribe(self, subscription: Subscription):
        self.subscription = subscription

    async def on_next(self, value: Payload, is_complete=False):
        logging.info('From client on channel: ' + value.data.decode('utf-8'))

    def on_error(self, exception: Exception):
        logging.error('Error on channel ' + str(exception))

    def on_complete(self, value=None):
        logging.info('Completed on channel')
