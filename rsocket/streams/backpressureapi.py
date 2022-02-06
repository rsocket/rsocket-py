import abc


class BackpressureApi(metaclass=abc.ABCMeta):
    @abc.abstractmethod
    def initial_request_n(self, n: int):
        ...
