import abc
import asyncio
import logging
from typing import AsyncGenerator, Any, Tuple

from reactivestreams.publisher import Publisher
from reactivestreams.subscriber import Subscriber
from reactivestreams.subscription import Subscription
from rsocket.payload import Payload


class QueueResponseStream(Publisher, Subscription, metaclass=abc.ABCMeta):

    def __init__(self):
        self._queue = asyncio.Queue()

    def subscribe(self, subscriber: Subscriber):
        subscriber.on_subscribe(self)
        self._subscriber = subscriber
        self._feeder = asyncio.ensure_future(self.feed())

    async def request(self, n: int):
        async for next_item in self.generate_next_n(n):
            await self._queue.put(next_item)

    @abc.abstractmethod
    async def generate_next_n(self, n: int) -> AsyncGenerator[Tuple[Any, bool], None]:
        yield None  # note: this line is here just to satisfy the IDEs' type checker

    def cancel(self):
        self._feeder.cancel()

    async def feed(self):

        try:
            while True:
                item, is_complete = await self._queue.get()
                await self._send(Payload(item.encode('utf-8')), is_complete)
                if is_complete:
                    break
        except asyncio.CancelledError:
            logging.debug('Canceled')

    async def _send(self, payload: Payload, is_complete=False):
        await self._subscriber.on_next(payload, is_complete)
