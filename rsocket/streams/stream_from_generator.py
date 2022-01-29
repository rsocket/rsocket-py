import abc
import asyncio
from datetime import timedelta
from io import BytesIO
from typing import AsyncGenerator, Tuple, Optional

from reactivestreams.publisher import Publisher
from reactivestreams.subscriber import Subscriber
from reactivestreams.subscription import Subscription
from rsocket.helpers import payload_to_n_size_fragments
from rsocket.logger import logger
from rsocket.payload import Payload


class StreamFromGenerator(Publisher, Subscription, metaclass=abc.ABCMeta):

    def __init__(self,
                 delay_between_messages=timedelta(0),
                 fragment_size: Optional[int] = None):
        self._queue = asyncio.Queue()
        self._fragment_size = fragment_size
        self._delay_between_messages = delay_between_messages
        self._subscriber = None
        self._feeder = None

    def subscribe(self, subscriber: Subscriber):
        subscriber.on_subscribe(self)
        self._subscriber = subscriber
        self._feeder = asyncio.ensure_future(self.feed_subscriber())

    def request(self, n: int):
        asyncio.create_task(self.queue_next_n(n))

    async def queue_next_n(self, n):
        async for next_item in self.generate_next_n(n):
            await self._queue.put(next_item)

    @abc.abstractmethod  # todo: move to accepting generator as __init__ argument
    async def generate_next_n(self, n: int) -> AsyncGenerator[Tuple[Payload, bool], None]:
        yield None  # note: this line is here just to satisfy the IDEs' type checker

    def cancel(self):
        self._feeder.cancel()

    async def feed_subscriber(self):

        try:
            while True:
                payload, is_complete = await self._queue.get()

                if self._fragment_size is None:
                    self._send_to_subscriber(payload, is_complete)
                else:
                    async for fragment in payload_to_n_size_fragments(BytesIO(payload.data),
                                                                      BytesIO(payload.metadata),
                                                                      self._fragment_size):
                        self._send_to_subscriber(fragment, is_complete and fragment.is_last)

                await asyncio.sleep(self._delay_between_messages.total_seconds())

                self._queue.task_done()
                if is_complete:
                    break
        except asyncio.CancelledError:
            logger().debug('Canceled')

    def _send_to_subscriber(self, payload: Payload, is_complete=False):
        self._subscriber.on_next(payload, is_complete)
