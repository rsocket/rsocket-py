import asyncio

from reactivestreams.subscriber import Subscriber
from reactivestreams.subscription import DefaultSubscription, Subscription
from rsocket.frame import MAX_REQUEST_N


class CollectorSubscriber(Subscriber, Subscription):

    def __init__(self, limit_rate=MAX_REQUEST_N, limit_count=None) -> None:
        self._limit_count = limit_count
        self._limit_rate = limit_rate
        self._received_count = 0
        self._total_received_count = 0
        self.is_done = asyncio.Event()
        self.error = None
        self.values = []
        self.subscription = None

    def on_complete(self):
        self.is_done.set()

    def on_subscribe(self, subscription: DefaultSubscription):
        self.subscription = subscription

    def cancel(self):
        self.subscription.cancel()

    def request(self, n: int):
        self.subscription.request(n)

    def on_next(self, value, is_complete=False):
        self.values.append(value)

        self._received_count += 1
        self._total_received_count += 1

        if is_complete:
            self.is_done.set()
        elif self._limit_count is not None and self._limit_count == self._total_received_count:
            self.subscription.cancel()
            self.is_done.set()
        else:
            if self._received_count == self._limit_rate:
                self._received_count = 0
                self.subscription.request(self._limit_rate)

    def on_error(self, exception: Exception):
        self.error = exception
        self.is_done.set()

    async def run(self):
        await self.is_done.wait()

        if self.error:
            raise self.error

        return self.values
