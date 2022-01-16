from asyncio import Future, ensure_future

from rsocket.frame import CancelFrame, Frame
from rsocket.streams.stream_handler import StreamHandler


class RequestResponseResponder(StreamHandler):
    def __init__(self, stream: int, socket, future: Future):
        super().__init__(stream, socket)
        self.future = future
        self.future.add_done_callback(self.future_done)

    def future_done(self, future):
        if self.future.cancelled():
            pass
        elif not future.exception():
            ensure_future(self.socket.send_payload(
                self.stream, future.result(), complete=True))
        else:
            ensure_future(self.socket.send_error(
                self.stream, future.exception()))
        self.socket.finish_stream(self.stream)

    async def frame_received(self, frame: Frame):
        if isinstance(frame, CancelFrame):
            self.future.cancel()
