import abc


class Disposable(metaclass=abc.ABCMeta):

    @abc.abstractmethod
    def dispose(self):
        ...
