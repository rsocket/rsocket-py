from abc import ABCMeta, abstractmethod


class Publisher(metaclass=ABCMeta):
    @abstractmethod
    def subscribe(self, subscriber):
        pass


class DefaultPublisher(Publisher):
    def subscribe(self, subscriber):
        subscriber.on_error(RuntimeError("Not implemented"))
