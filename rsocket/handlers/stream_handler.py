from abc import abstractmethod, ABCMeta

from rsocket.frame import CancelFrame, RequestNFrame, \
    RequestStreamFrame, Frame, KeepAliveFrame
from rsocket.handlers.stream import Stream
from rsocket.payload import Payload

MAX_REQUEST_N = 0x7FFFFFFF


class StreamHandler(Stream, metaclass=ABCMeta):
    def __init__(self, stream: int, socket):
        super().__init__()
        self.stream = stream
        self.socket = socket
        self._initial_request_n = MAX_REQUEST_N

    def limit_rate(self, n: int):
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

        self.socket.send_frame(request)

    def send_request_keepalive(self):
        frame = KeepAliveFrame()
        frame.stream_id = 0
        frame.data = b''
        frame.metadata = b''
        frame.flags_respond = True

        self.socket.send_frame(frame)
