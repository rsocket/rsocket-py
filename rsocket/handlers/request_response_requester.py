import asyncio
from asyncio import Future

from rsocket.frame import ErrorFrame, PayloadFrame, Frame, error_frame_to_exception
from rsocket.frame_builders import to_request_response_frame
from rsocket.helpers import create_future
from rsocket.payload import Payload
from rsocket.rsocket_interface import RSocketInterface
from rsocket.streams.stream_handler import StreamHandler


class RequestResponseRequester(StreamHandler):
    def __init__(self, socket: RSocketInterface, payload: Payload):
        super().__init__(socket)
        self._payload = payload
        self._future = create_future()

    def setup(self):
        self._future.add_done_callback(self._on_future_complete)

    def run(self) -> Future:
        request = to_request_response_frame(self.stream_id, self._payload)
        self.socket.send_request(request)
        return self._future

    def frame_received(self, frame: Frame):
        if isinstance(frame, PayloadFrame):
            self._future.set_result(Payload(frame.data, frame.metadata))
            self._finish_stream()
        elif isinstance(frame, ErrorFrame):
            self._future.set_exception(error_frame_to_exception(frame))
            self._finish_stream()

    def _on_future_complete(self, future: asyncio.Future):
        if future.cancelled():
            self.cancel()

    def cancel(self):
        self.send_cancel()
