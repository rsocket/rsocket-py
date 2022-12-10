import abc

from reactivestreams.subscriber import Subscriber


class Publisher(metaclass=abc.ABCMeta):
    """
    Handles event for subscription to a subscriber
    """

    @abc.abstractmethod
    def subscribe(self, subscriber: Subscriber):
        ...


class DefaultPublisher(Publisher):
    def subscribe(self, subscriber: Subscriber):
        self._subscriber = subscriber
