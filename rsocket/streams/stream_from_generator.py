import asyncio
from datetime import timedelta
from typing import AsyncGenerator, Tuple, Optional, Callable, Generator

from reactivestreams.subscriber import Subscriber
from rsocket.async_helpers import async_range
from rsocket.disposable import Disposable
from rsocket.helpers import DefaultPublisherSubscription
from rsocket.logger import logger
from rsocket.payload import Payload

__all__ = ['StreamFromGenerator']

from rsocket.streams.exceptions import FinishedIterator

_finished_iterator = object()


class StreamFromGenerator(DefaultPublisherSubscription, Disposable):

    def __init__(self,
                 generator: Callable[[], Generator[Tuple[Payload, bool], None, None]],
                 delay_between_messages=timedelta(0),
                 on_cancel=None,
                 on_complete=None):
        self._generator_factory = generator
        self._queue = asyncio.Queue()
        self._delay_between_messages = delay_between_messages
        self._subscriber: Optional[Subscriber] = None
        self._payload_feeder = None
        self._iteration = None
        self._request_n_queue = asyncio.Queue()
        self._n_feeder = None
        self._on_complete = on_complete
        self._on_cancel = on_cancel

    async def _start_generator(self):
        self._generator = self._generator_factory()
        self._iteration = iter(self._generator)

    def dispose(self):
        self._generator.close()

    def subscribe(self, subscriber: Subscriber):
        super().subscribe(subscriber)

        if self._payload_feeder is None:
            self._payload_feeder = asyncio.create_task(self.feed_subscriber())

    def request(self, n: int):
        if self._n_feeder is None:
            self._n_feeder = asyncio.create_task(self.queue_next_n())

        self._request_n_queue.put_nowait(n)

    async def queue_next_n(self):
        try:
            await self._start_generator()

            while True:
                n = await self._request_n_queue.get()

                async for payload, is_complete in self._generate_next_n(n):
                    self._queue.put_nowait((payload, is_complete))
                    if is_complete:
                        return

        except FinishedIterator:
            self._queue.put_nowait((Payload(), True))
        except asyncio.CancelledError:
            logger().debug('Asyncio task canceled: queue_next_n')
        except Exception as exception:
            logger().error('Stream error', exc_info=True)
            self._subscriber.on_error(exception)
            self._cancel_feeders()

    async def _generate_next_n(self, n: int) -> AsyncGenerator[Tuple[Payload, bool], None]:
        is_complete_sent = False
        async for i in async_range(n):
            next_value = next(self._iteration, _finished_iterator)

            if next_value is _finished_iterator:
                if not is_complete_sent:
                    raise FinishedIterator()
                return

            is_complete_sent = next_value[1]
            yield next_value

    def cancel(self):
        self._cancel_feeders()

        if self._generator is not None:
            self._cancel_generator()

        if self._on_cancel is not None:
            self._on_cancel()

    def _cancel_feeders(self):
        self._cancel_payload_feeder()
        self._cancel_n_feeder()

    def _cancel_payload_feeder(self):
        if self._payload_feeder is not None:
            self._payload_feeder.cancel()
            self._payload_feeder = None

    def _cancel_n_feeder(self):
        if self._n_feeder is not None:
            self._n_feeder.cancel()
            self._n_feeder = None

    async def feed_subscriber(self):

        try:
            while True:
                payload, is_complete = await self._queue.get()

                self._send_to_subscriber(payload, is_complete)

                await asyncio.sleep(self._delay_between_messages.total_seconds())

                self._queue.task_done()

                if is_complete:
                    if self._on_complete is not None:
                        self._on_complete()
                    break
        except asyncio.CancelledError:
            logger().debug('Asyncio task canceled: stream_from_generator')
        finally:
            self._cancel_n_feeder()

    def _send_to_subscriber(self, payload: Optional[Payload], is_complete=False):
        if payload is None and is_complete:
            self._subscriber.on_complete()
        else:
            self._subscriber.on_next(payload, is_complete)

    def _cancel_generator(self):
        self._generator.close()
