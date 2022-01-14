from abc import ABCMeta, abstractmethod

from reactivestreams.subscriber import Subscriber


class Publisher(metaclass=ABCMeta):
    @abstractmethod
    def subscribe(self, subscriber: Subscriber):
        ...
