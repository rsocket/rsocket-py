from abc import ABCMeta, abstractmethod


class Subscriber(metaclass=ABCMeta):
    @abstractmethod
    def on_subscribe(self, subscription):
        pass

    @abstractmethod
    def on_next(self, value):
        pass

    @abstractmethod
    def on_error(self, exception):
        pass

    @abstractmethod
    def on_complete(self):
        pass


class DefaultSubscriber(Subscriber):
    def on_next(self, value):
        pass

    def on_error(self, exception):
        pass

    def on_subscribe(self, subscription):
        pass

    def on_complete(self):
        pass