from abc import ABCMeta, abstractmethod

from reactivestreams.subscription import Subscription


class Publisher(metaclass=ABCMeta):
    @abstractmethod
    def subscribe(self, subscriber):
        ...


class DefaultPublisher(Publisher, Subscription):
    async def request(self, n: int):
        pass

    def cancel(self):
        pass

    def subscribe(self, subscriber):
        subscriber.on_subscribe(self)
        subscriber.on_error(RuntimeError("Not implemented"))
