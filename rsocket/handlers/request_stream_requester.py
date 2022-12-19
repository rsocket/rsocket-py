from reactivestreams.subscriber import Subscriber
from rsocket.frame import ErrorFrame, PayloadFrame, Frame, error_frame_to_exception
from rsocket.frame_builders import to_request_stream_frame
from rsocket.handlers.interfaces import Requester
from rsocket.helpers import payload_from_frame, DefaultPublisherSubscription
from rsocket.payload import Payload
from rsocket.rsocket import RSocket
from rsocket.streams.stream_handler import StreamHandler


class RequestStreamRequester(StreamHandler, DefaultPublisherSubscription, Requester):
    def __init__(self, socket: RSocket, payload: Payload):
        super().__init__(socket)
        self.payload = payload

    def setup(self):
        pass

    def subscribe(self, subscriber: Subscriber):
        super().subscribe(subscriber)
        self._send_stream_request(self.payload)

    def cancel(self):
        self.send_cancel()
        self._finish_stream()

    def request(self, n: int):
        self.send_request_n(n)

    def frame_received(self, frame: Frame):
        if isinstance(frame, PayloadFrame):
            if frame.flags_next:
                self._subscriber.on_next(payload_from_frame(frame),
                                         is_complete=frame.flags_complete)
            elif frame.flags_complete:
                self._subscriber.on_complete()

            if frame.flags_complete:
                self._finish_stream()
        elif isinstance(frame, ErrorFrame):
            self._subscriber.on_error(error_frame_to_exception(frame))
            self._finish_stream()

    def _send_stream_request(self, payload: Payload):
        self.socket.send_request(to_request_stream_frame(
            stream_id=self.stream_id,
            payload=payload,
            initial_request_n=self._initial_request_n,
            fragment_size_bytes=self.socket.get_fragment_size_bytes()
        ))
