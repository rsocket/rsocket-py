import asyncio

from rsocket.frame import ErrorFrame, PayloadFrame, Frame, error_frame_to_exception
from rsocket.frame_builders import to_request_response_frame
from rsocket.handlers.interfaces import Requester
from rsocket.helpers import create_future, payload_from_frame
from rsocket.local_typing import Awaitable
from rsocket.payload import Payload
from rsocket.rsocket import RSocket
from rsocket.streams.stream_handler import StreamHandler


class RequestResponseRequester(StreamHandler, Requester):
    def __init__(self, socket: RSocket, payload: Payload):
        super().__init__(socket)
        self._payload = payload
        self._future = create_future()

    def setup(self):
        self._future.add_done_callback(self._on_future_complete)

    def run(self) -> Awaitable[Payload]:
        request = to_request_response_frame(self.stream_id,
                                            self._payload,
                                            self.socket.get_fragment_size_bytes())
        self.socket.send_request(request)
        return self._future

    def frame_received(self, frame: Frame):
        if isinstance(frame, PayloadFrame):
            self._future.set_result(payload_from_frame(frame))
            self._finish_stream()
        elif isinstance(frame, ErrorFrame):
            self._future.set_exception(error_frame_to_exception(frame))
            self._finish_stream()

    def _on_future_complete(self, future: asyncio.Future):
        if future.cancelled():
            self.cancel()

    def cancel(self):
        self.send_cancel()
        self._finish_stream()
