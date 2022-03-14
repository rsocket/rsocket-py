import abc

from rsocket.frame import Frame
from rsocket.frame_parser import FrameParser


class Transport(metaclass=abc.ABCMeta):

    def __init__(self):
        self._frame_parser = FrameParser()

    @abc.abstractmethod
    async def connect(self):
        ...

    @abc.abstractmethod
    async def send_frame(self, frame: Frame):
        ...

    @abc.abstractmethod
    async def next_frame_generator(self, is_server_alive):
        ...

    @abc.abstractmethod
    async def close(self):
        ...

    async def on_send_queue_empty(self):
        pass
