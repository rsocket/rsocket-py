import abc
import asyncio
from typing import AsyncGenerator, Tuple

from reactivestreams.publisher import Publisher
from reactivestreams.subscriber import Subscriber
from reactivestreams.subscription import Subscription
from rsocket.fragment import Fragment
from rsocket.logger import logger
from rsocket.payload import Payload


class ResponseStreamFromGenerator(Publisher, Subscription, metaclass=abc.ABCMeta):

    def __init__(self):
        self._queue = asyncio.Queue()
        self._subscriber = None
        self._feeder = None

    def subscribe(self, subscriber: Subscriber):
        subscriber.on_subscribe(self)
        self._subscriber = subscriber
        self._feeder = asyncio.ensure_future(self.feed_subscriber())

    async def request(self, n: int):
        async for next_item in self.generate_next_n(n):
            await self._queue.put(next_item)

    @abc.abstractmethod  # todo: move to accepting generator as __init__ argument
    async def generate_next_n(self, n: int) -> AsyncGenerator[Tuple[Fragment, bool], None]:
        yield None  # note: this line is here just to satisfy the IDEs' type checker

    def cancel(self):
        self._feeder.cancel()

    async def feed_subscriber(self):

        try:
            while True:
                item, is_complete = await self._queue.get()
                await self._send_to_subscriber(item, is_complete)
                if is_complete:
                    break
        except asyncio.CancelledError:
            logger().debug('Canceled')

    async def _send_to_subscriber(self, payload: Payload, is_complete=False):
        await self._subscriber.on_next(payload, is_complete)
