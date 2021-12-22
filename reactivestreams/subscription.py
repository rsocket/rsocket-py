from abc import ABCMeta, abstractmethod


class Subscription(metaclass=ABCMeta):
    @abstractmethod
    def request(self, n: int):
        ...

    @abstractmethod
    def cancel(self):
        ...
