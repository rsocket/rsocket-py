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
        self._feeder = asyncio.ensure_future(self.feed(subscriber))

    async def request(self, n: int):
        async for next_item in self.generate_next_n(n):
            await self._queue.put(next_item)

    @abc.abstractmethod
    async def generate_next_n(self, n: int) -> AsyncGenerator[Tuple[Any, bool], None]:
        ...

    def cancel(self):
        self._feeder.cancel()

    async def feed(self, subscriber):
        loop = asyncio.get_event_loop()
        try:
            while True:
                item, is_complete = await self._queue.get()
                loop.call_soon(subscriber.on_next, Payload(item.encode('utf-8')), is_complete)
        except asyncio.CancelledError:
            logging.debug("Canceled")
