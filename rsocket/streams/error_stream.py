from reactivestreams.publisher import Publisher
from reactivestreams.subscriber import Subscriber
from reactivestreams.subscription import DefaultSubscription


class ErrorStream(Publisher, DefaultSubscription):
    async def request(self, n: int):
        self._subscriber.on_error(self._exception)

    def __init__(self, exception: Exception):
        self._exception = exception

    def subscribe(self, subscriber: Subscriber):
        subscriber.on_subscribe(self)
        self._subscriber = subscriber
