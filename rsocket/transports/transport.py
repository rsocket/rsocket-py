import abc

from rsocket.frame import Frame


class Transport(metaclass=abc.ABCMeta):

    @abc.abstractmethod
    async def on_send_queue_empty(self):
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

    @abc.abstractmethod
    async def close_writer(self):
        ...
