from typing import AsyncGenerator, Tuple, Any

from reactivestreams.subscriber import Subscriber
from reactivestreams.subscription import Subscription
from rsocket import Payload
from rsocket.queue_response_stream import QueueResponseStream


class ResponseChannel(QueueResponseStream, Subscriber):
    def __init__(self, response_count: int = 3):
        super().__init__()
        self._response_count = response_count
        self._current_response = 0

    async def generate_next_n(self, n: int) -> AsyncGenerator[Tuple[Any, bool], None]:
        for i in range(n):
            is_complete = (self._current_response + 1) == self._response_count

            yield ("Item: %s" % self._current_response), is_complete

            await self.subscription.request(1)
            if is_complete:
                break

            self._current_response += 1

    def on_subscribe(self, subscription: Subscription):
        self.subscription = subscription

    def on_next(self, value: Payload, is_complete=False):
        print("From client: " + value.data.decode('utf-8'))

    def on_error(self, exception: Exception):
        print("Error " + str(exception))
        raise exception

    def on_complete(self, value=None):
        print("Completed")
