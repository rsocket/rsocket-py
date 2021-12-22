from abc import ABCMeta, abstractmethod


class Publisher(metaclass=ABCMeta):
    @abstractmethod
    def subscribe(self, subscriber):
        ...


class DefaultPublisher(Publisher):
    def subscribe(self, subscriber):
        subscriber.on_error(RuntimeError("Not implemented"))
