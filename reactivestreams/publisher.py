import abc

from reactivestreams.subscriber import Subscriber


class Publisher(metaclass=abc.ABCMeta):
    @abc.abstractmethod
    def subscribe(self, subscriber: Subscriber):
        ...
