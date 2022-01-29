from reactivestreams.publisher import Publisher
from reactivestreams.subscription import Subscription
from rsocket.frame import ErrorFrame, PayloadFrame, Frame, error_frame_to_exception
from rsocket.streams.stream_handler import StreamHandler
from rsocket.payload import Payload


class RequestStreamRequester(StreamHandler, Publisher, Subscription):
    def __init__(self, stream: int, socket, payload: Payload):
        super().__init__(stream, socket)
        self.payload = payload

    def subscribe(self, subscriber):
        # noinspection PyAttributeOutsideInit
        self.subscriber = subscriber
        self.send_stream_request(self.payload)
        self.subscriber.on_subscribe(self)

    def cancel(self):
        super().cancel()
        self.send_cancel()

    def request(self, n: int):
        self.send_request_n(n)

    async def frame_received(self, frame: Frame):
        if isinstance(frame, PayloadFrame):
            if frame.flags_next:
                self.subscriber.on_next(Payload(frame.data, frame.metadata))
            if frame.flags_complete:
                self.subscriber.on_complete()
                self.socket.finish_stream(self.stream)
        elif isinstance(frame, ErrorFrame):
            self.subscriber.on_error(error_frame_to_exception(frame))
            self.socket.finish_stream(self.stream)
