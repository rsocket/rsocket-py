import asyncio

from reactivestreams.publisher import Publisher
from reactivestreams.subscriber import Subscriber
from reactivestreams.subscription import Subscription
from rsocket.payload import Payload
from rsocket.request_handler import BaseRequestHandler


class ResponseStream(BaseRequestHandler, Publisher, Subscription):

    def subscribe(self, subscriber: Subscriber):
        subscriber.on_subscribe(self)
        self.feeder = asyncio.ensure_future(self.feed(subscriber))

    def request(self, n: int):
        pass

    def cancel(self):
        self.feeder.cancel()

    async def feed(self, subscriber):
        loop = asyncio.get_event_loop()
        try:
            for x in range(3):
                value = Payload('Feed Item: {}'.format(x).encode('utf-8'))
                loop.call_soon(subscriber.on_next, value)
            loop.call_soon(subscriber.on_complete)
        except asyncio.CancelledError:
            pass
