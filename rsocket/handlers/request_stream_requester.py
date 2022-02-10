from reactivestreams.publisher import Publisher
from reactivestreams.subscriber import Subscriber
from reactivestreams.subscription import Subscription
from rsocket.frame import ErrorFrame, PayloadFrame, Frame, error_frame_to_exception
from rsocket.frame_builders import to_request_stream_frame
from rsocket.logger import logger
from rsocket.payload import Payload
from rsocket.streams.stream_handler import StreamHandler


class RequestStreamRequester(StreamHandler, Publisher, Subscription):
    def __init__(self, stream: int, socket, payload: Payload):
        super().__init__(stream, socket)
        self.payload = payload

    def subscribe(self, subscriber: Subscriber):
        # noinspection PyAttributeOutsideInit
        self.subscriber = subscriber
        self._send_stream_request(self.payload)
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

    def _send_stream_request(self, payload: Payload):
        logger().debug('%s: Sending stream request: %s', self.socket._log_identifier(), payload)

        self.socket.send_request(to_request_stream_frame(
            self.stream,
            payload,
            self._initial_request_n
        ))
