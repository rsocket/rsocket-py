from abc import ABCMeta, abstractmethod
from typing import Optional

from reactivestreams.subscription import Subscription


class Subscriber(metaclass=ABCMeta):
    """
    Handles stream events.
    """

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
    def __init__(self, on_next=None,
                 on_error=None,
                 on_complete=None,
                 on_subscribe=None):
        self._on_subscribe = on_subscribe
        self._on_complete = on_complete
        self._on_error = on_error
        self._on_next = on_next
        self.subscription: Optional[Subscription] = None

    def on_next(self, value, is_complete=False):
        if self._on_next is not None:
            self._on_next(value, is_complete)

    def on_error(self, exception: Exception):
        if self._on_error is not None:
            self._on_error(exception)

    def on_subscribe(self, subscription: Subscription):
        self.subscription = subscription

        if self._on_subscribe is not None:
            self._on_subscribe(subscription)

    def on_complete(self):
        if self._on_complete is not None:
            self._on_complete()
