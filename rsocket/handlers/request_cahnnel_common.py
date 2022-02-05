from typing import Optional

from reactivestreams.publisher import Publisher
from reactivestreams.subscriber import Subscriber
from reactivestreams.subscription import Subscription
from rsocket.frame import CancelFrame, ErrorFrame, RequestNFrame, \
    PayloadFrame, Frame, error_frame_to_exception
from rsocket.payload import Payload
from rsocket.streams.stream_handler import StreamHandler


class RequestChannelCommon(StreamHandler, Publisher, Subscription):
    class StreamSubscriber(Subscriber):
        def __init__(self, stream: int, socket, requester: 'RequestChannelCommon'):
            super().__init__()
            self._stream = stream
            self._socket = socket
            self._requester = requester

        def on_next(self, value, is_complete=False):
            self._socket.send_payload(
                self._stream, value, complete=is_complete)

        def on_complete(self):
            self._socket.send_payload(
                self._stream, Payload(b'', b''), complete=True)
            self._requester._sent_complete = True
            self._requester._finish_if_both_closed()

        def on_error(self, exception):
            self._socket.send_error(self._stream, exception)
            self._requester._sent_complete = True
            self._requester._finish_if_both_closed()

        def on_subscribe(self, subscription):
            # noinspection PyAttributeOutsideInit
            self.subscription = subscription

    def __init__(self, stream: int, socket, local_publisher: Optional[Publisher] = None):
        super().__init__(stream, socket)
        self._sent_complete = False
        self._received_complete = False
        self.local_publisher = local_publisher
        self.subscriber = self.StreamSubscriber(stream, socket, self)
        self.local_publisher.subscribe(self.subscriber)

    async def frame_received(self, frame: Frame):
        if isinstance(frame, CancelFrame):
            self.subscriber.subscription.cancel()
        elif isinstance(frame, RequestNFrame):
            self.subscriber.subscription.request(frame.request_n)

        elif isinstance(frame, PayloadFrame):
            if frame.flags_next:
                self.local_subscriber.on_next(Payload(frame.data, frame.metadata))
            if frame.flags_complete:
                self._complete_local_subscriber()
        elif isinstance(frame, ErrorFrame):
            self.local_subscriber.on_error(error_frame_to_exception(frame))
            self._received_complete = True
            self._finish_if_both_closed()

    def _complete_local_subscriber(self):
        self.local_subscriber.on_complete()
        self._received_complete = True
        self._finish_if_both_closed()

    def _finish_stream(self):
        self.socket.finish_stream(self.stream)

    def _finish_if_both_closed(self):
        if self._received_complete and self._sent_complete:
            self._finish_stream()

    def subscribe(self, subscriber: Subscriber):
        self.local_subscriber = subscriber
        self.local_subscriber.on_subscribe(self)

    def cancel(self):
        self.send_cancel()

    def request(self, n: int):
        self.send_request_n(n)
