from asyncio import Future

from rsocket.frame import ErrorFrame, PayloadFrame, Frame, error_frame_to_exception
from rsocket.frame_builders import to_request_response_frame
from rsocket.payload import Payload
from rsocket.streams.stream_handler import StreamHandler


class RequestResponseRequester(StreamHandler, Future):
    def __init__(self, stream: int, socket, payload: Payload):
        super().__init__(stream, socket)
        stream_id = self.stream
        request = to_request_response_frame(stream_id, payload)
        self.socket.send_request(request)

    async def frame_received(self, frame: Frame):
        if isinstance(frame, PayloadFrame):
            self.set_result(Payload(frame.data, frame.metadata))
            self.socket.finish_stream(self.stream)
        elif isinstance(frame, ErrorFrame):
            self.set_exception(error_frame_to_exception(frame))
            self.socket.finish_stream(self.stream)

    def cancel(self, *args, **kwargs):
        super().cancel(*args, **kwargs)
        self.send_cancel()
