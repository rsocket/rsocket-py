from abc import abstractmethod, ABCMeta

from rsocket.exceptions import RSocketValueErrorException
from rsocket.frame import Frame, MAX_REQUEST_N
from rsocket.frame_builders import to_cancel_frame, to_request_n_frame
from rsocket.logger import logger
from rsocket.streams.backpressureapi import BackpressureApi


class StreamHandler(BackpressureApi, metaclass=ABCMeta):
    def __init__(self, stream: int, socket):
        super().__init__()
        self.stream = stream
        self.socket = socket
        self._initial_request_n = MAX_REQUEST_N

    def initial_request_n(self, n: int):
        if n <= 0:
            raise RSocketValueErrorException('Initial request N must be > 0')

        self._initial_request_n = n
        return self

    def frame_sent(self, frame: Frame):
        """Not being marked abstract, since most handlers won't override."""

    @abstractmethod
    async def frame_received(self, frame: Frame):
        ...

    def send_cancel(self):
        """Convenience method for use by requester subclasses."""
        logger().debug('%s: Sending cancel', self.socket._log_identifier())

        self.socket.send_frame(to_cancel_frame(self.stream))
        self.socket.finish_stream(self.stream)

    def send_request_n(self, n: int):
        logger().debug('%s: Sending request N: %d', self.socket._log_identifier(), n)

        self.socket.send_frame(to_request_n_frame(self.stream, n))
