from abc import ABCMeta, abstractmethod


class Subscription(metaclass=ABCMeta):
    @abstractmethod
    def request(self, n):
        pass

    @abstractmethod
    def cancel(self):
        pass
