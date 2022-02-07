from abc import abstractmethod, ABCMeta

from rsocket.exceptions import RSocketValueErrorException
from rsocket.frame import CancelFrame, RequestNFrame, \
    RequestStreamFrame, Frame
from rsocket.payload import Payload
from rsocket.streams.backpressureapi import BackpressureApi

MAX_REQUEST_N = 0x7FFFFFFF


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
        frame = CancelFrame()
        frame.stream_id = self.stream
        self.socket.send_frame(frame)
        self.socket.finish_stream(self.stream)

    def send_request_n(self, n: int):
        """Convenience method for use by requester subclasses."""
        frame = RequestNFrame()
        frame.stream_id = self.stream
        frame.request_n = n

        self.socket.send_frame(frame)

    def send_stream_request(self, payload: Payload):
        request = RequestStreamFrame()
        request.initial_request_n = self._initial_request_n
        request.stream_id = self.stream
        request.data = payload.data
        request.metadata = payload.metadata

        self.socket.send_request(request)
