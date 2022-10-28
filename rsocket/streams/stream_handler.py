from abc import abstractmethod, ABCMeta
from typing import Optional, Union

from rsocket.exceptions import RSocketValueError
from rsocket.frame import Frame, MAX_REQUEST_N
from rsocket.frame_builders import to_cancel_frame, to_request_n_frame
from rsocket.rsocket import RSocket
from rsocket.rsocket_internal import RSocketInternal
from rsocket.streams.backpressureapi import BackpressureApi


class StreamHandler(BackpressureApi, metaclass=ABCMeta):
    def __init__(self, socket: Union[RSocket, RSocketInternal]):
        super().__init__()
        self.stream_id: Optional[int] = None
        self.socket = socket
        self._initial_request_n = MAX_REQUEST_N

    @abstractmethod
    def setup(self):
        ...

    def initial_request_n(self, n: int):
        if n <= 0:
            self.socket.finish_stream(self.stream_id)
            raise RSocketValueError('Initial request N must be > 0')

        self._initial_request_n = n
        return self

    def frame_sent(self, frame: Frame):
        """Not being marked abstract, since most handlers won't override."""

    @abstractmethod
    def frame_received(self, frame: Frame):
        ...

    def send_cancel(self):
        """Convenience method for use by requester subclasses."""

        self.socket.send_frame(to_cancel_frame(self.stream_id))
        self._finish_stream()

    def send_request_n(self, n: int):
        self.socket.send_frame(to_request_n_frame(self.stream_id, n))

    def _finish_stream(self):
        self.socket.finish_stream(self.stream_id)
