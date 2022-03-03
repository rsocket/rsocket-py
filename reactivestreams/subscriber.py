from abc import ABCMeta, abstractmethod
from typing import Optional

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
    def __init__(self):
        self.subscription: Optional[Subscription] = None

    def on_next(self, value, is_complete=False):
        pass

    def on_error(self, exception: Exception):
        pass

    def on_subscribe(self, subscription: Subscription):
        self.subscription = subscription

    def on_complete(self):
        pass
