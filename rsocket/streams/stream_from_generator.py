import abc
import asyncio
from datetime import timedelta
from io import BytesIO
from typing import AsyncGenerator, Tuple, Optional

from reactivestreams.publisher import Publisher
from reactivestreams.subscriber import Subscriber
from reactivestreams.subscription import Subscription
from rsocket.frame_helpers import payload_to_n_size_fragments
from rsocket.logger import logger
from rsocket.payload import Payload


class StreamFromGenerator(Publisher, Subscription, metaclass=abc.ABCMeta):

    def __init__(self,
                 generator,
                 delay_between_messages=timedelta(0),
                 fragment_size: Optional[int] = None,
                 on_cancel=None):
        self._generator = generator
        self._queue = asyncio.Queue()
        self._fragment_size = fragment_size
        self._delay_between_messages = delay_between_messages
        self._subscriber: Optional[Subscriber] = None
        self._feeder = None
        self._iteration = None
        self._on_cancel = on_cancel

    async def _start_generator(self):
        self._iteration = iter(self._generator())

    def subscribe(self, subscriber: Subscriber):
        subscriber.on_subscribe(self)
        self._subscriber = subscriber
        self._feeder = asyncio.ensure_future(self.feed_subscriber())

    def request(self, n: int):
        self._n_feeder = asyncio.create_task(self.queue_next_n(n))

    async def queue_next_n(self, n):
        try:
            if self._iteration is None:
                await self._start_generator()

            async for next_item in self._generate_next_n(n):
                await self._queue.put(next_item)
        except asyncio.CancelledError:
            pass
        except Exception as exception:
            self._subscriber.on_error(exception)
            self._cancel_feeders()

    async def _generate_next_n(self, n: int) -> AsyncGenerator[Tuple[Payload, bool], None]:
        for i in range(n):
            try:
                yield next(self._iteration)
            except StopIteration:
                return

    def cancel(self):
        self._cancel_feeders()

        if self._on_cancel is not None:
            self._on_cancel()

    def _cancel_feeders(self):
        if self._feeder is not None:
            self._feeder.cancel()
        if self._n_feeder is not None:
            self._n_feeder.cancel()

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
            logger().debug('Asyncio task canceled')

    def _send_to_subscriber(self, payload: Payload, is_complete=False):
        self._subscriber.on_next(payload, is_complete)
