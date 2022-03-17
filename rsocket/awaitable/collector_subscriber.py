import asyncio

from reactivestreams.subscriber import Subscriber
from reactivestreams.subscription import DefaultSubscription


class CollectorSubscriber(Subscriber):

    def __init__(self) -> None:
        self.is_done = asyncio.Event()
        self.error = None
        self.values = []
        self.subscription = None

    def on_complete(self):
        self.is_done.set()

    def on_subscribe(self, subscription: DefaultSubscription):
        self.subscription = subscription

    def on_next(self, value, is_complete=False):
        self.values.append(value)

        if is_complete:
            self.is_done.set()

    def on_error(self, exception: Exception):
        self.error = exception
        self.is_done.set()

    async def run(self):
        await self.is_done.wait()

        if self.error:
            raise self.error

        return self.values
