import asyncio
import logging

from reactivestreams.publisher import Publisher
from reactivestreams.subscriber import Subscriber
from reactivestreams.subscription import Subscription
from rsocket.payload import Payload


class ResponseStream(Publisher, Subscription):

    def __init__(self, response_count=3):
        self._response_count = response_count
        self._current_response = 0
        self._queue = asyncio.Queue()

    def subscribe(self, subscriber: Subscriber):
        subscriber.on_subscribe(self)
        self.feeder = asyncio.ensure_future(self.feed(subscriber))

    async def request(self, n: int):
        for i in range(n):
            if self._current_response == self._response_count:
                break
            await self._push_next_response()

    async def _push_next_response(self):
        await self._queue.put(self._current_response)
        self._current_response += 1

    def cancel(self):
        self.feeder.cancel()

    async def feed(self, subscriber):
        loop = asyncio.get_event_loop()
        try:
            while True:
                item = await self._queue.get()
                value = Payload('Feed Item: {}'.format(item).encode('utf-8'))
                is_complete = item == self._response_count - 1
                loop.call_soon(subscriber.on_next, value, is_complete)
        except asyncio.CancelledError:
            logging.debug("Canceled")
