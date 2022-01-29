from abc import ABCMeta, abstractmethod

from reactivestreams.subscription import Subscription


class Subscriber(metaclass=ABCMeta):
    @abstractmethod
    def on_subscribe(self, subscription: Subscription):
        ...

    @abstractmethod
    def on_next(self, value, is_complete=False):
        ...

    @abstractmethod
    def on_error(self, exception: Exception):
        ...

    @abstractmethod
    def on_complete(self):
        ...


class DefaultSubscriber(Subscriber):
    def on_next(self, value, is_complete=False):
        pass

    def on_error(self, exception: Exception):
        pass

    def on_subscribe(self, subscription: Subscription):
        pass

    def on_complete(self):
        pass
