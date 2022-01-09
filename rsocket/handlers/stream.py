import abc


class Stream(metaclass=abc.ABCMeta):
    @abc.abstractmethod
    def limit_rate(self, n: int):
        ...
