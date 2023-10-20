import abc

from rsocket.frame import Frame


class Requester(metaclass=abc.ABCMeta):

    @abc.abstractmethod
    def frame_received(self, frame: Frame):
        ...
