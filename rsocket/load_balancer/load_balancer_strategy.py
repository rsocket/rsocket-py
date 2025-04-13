import abc

from rsocket.rsocket import RSocket


class LoadBalancerStrategy(metaclass=abc.ABCMeta):
    """
    Base class for load balancer strategies.
    """

    @abc.abstractmethod
    def select(self) -> RSocket:
        ...

    @abc.abstractmethod
    async def connect(self):
        ...

    @abc.abstractmethod
    async def close(self):
        ...
