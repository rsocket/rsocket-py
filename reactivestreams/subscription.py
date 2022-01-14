from abc import ABCMeta, abstractmethod


class Subscription(metaclass=ABCMeta):
    @abstractmethod
    async def request(self, n: int):
        ...

    @abstractmethod
    def cancel(self):
        ...


class DefaultSubscription(Subscription):
    async def request(self, n: int):
        pass

    def cancel(self):
        pass
