from reactivestreams.publisher import Publisher
from reactivestreams.subscriber import DefaultSubscriber
from rsocket.frame import CancelFrame, RequestNFrame, \
    RequestStreamFrame, Frame
from rsocket.payload import Payload
from rsocket.rsocket import RSocket
from rsocket.streams.stream_handler import StreamHandler


class StreamSubscriber(DefaultSubscriber):
    def __init__(self, stream_id: int, socket):
        super().__init__()
        self.stream_id = stream_id
        self.socket = socket

    def on_next(self, value: Payload, is_complete=False):
        self.socket.send_payload(
            self.stream_id, value, complete=is_complete)

        if is_complete:
            self.socket.finish_stream(self.stream_id)

    def on_complete(self):
        self.socket.send_payload(
            self.stream_id, Payload(), complete=True, is_next=False)

        self.socket.finish_stream(self.stream_id)

    def on_error(self, exception: Exception):
        self.socket.send_error(self.stream_id, exception)
        self.socket.finish_stream(self.stream_id)


class RequestStreamResponder(StreamHandler):

    def __init__(self, socket: RSocket, publisher: Publisher):
        super().__init__(socket)
        self.publisher = publisher
        self.subscriber = None

    def setup(self):
        self.subscriber = StreamSubscriber(self.stream_id, self.socket)
        self.publisher.subscribe(self.subscriber)

    def frame_received(self, frame: Frame):
        if isinstance(frame, RequestStreamFrame):
            self.setup()
            self.subscriber.subscription.request(frame.initial_request_n)
        elif isinstance(frame, CancelFrame):
            self.subscriber.subscription.cancel()
            self._finish_stream()
        elif isinstance(frame, RequestNFrame):
            self.subscriber.subscription.request(frame.request_n)
