from asyncio import Future

from rsocket.disposable import Disposable
from rsocket.frame import CancelFrame, Frame
from rsocket.rsocket import RSocket
from rsocket.streams.stream_handler import StreamHandler


class RequestResponseResponder(StreamHandler, Disposable):
    def __init__(self, socket: RSocket, future: Future):
        super().__init__(socket)
        self.future = future

    def setup(self):
        self.future.add_done_callback(self.future_done)

    def future_done(self, future):
        if self.future.cancelled():
            pass
        elif not future.exception():
            self.socket.send_payload(
                self.stream_id, future.result(), complete=True)
        else:
            self.socket.send_error(self.stream_id, future.exception())

        self._finish_stream()

    def dispose(self):
        self.future.cancel()

    def frame_received(self, frame: Frame):
        if isinstance(frame, CancelFrame):
            self.future.cancel()
            self._finish_stream()
