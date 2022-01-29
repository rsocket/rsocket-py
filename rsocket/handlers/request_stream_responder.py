from reactivestreams.publisher import Publisher
from reactivestreams.subscriber import Subscriber
from reactivestreams.subscription import Subscription
from rsocket.frame import CancelFrame, RequestNFrame, \
    RequestStreamFrame, Frame
from rsocket.payload import Payload
from rsocket.streams.stream_handler import StreamHandler


class RequestStreamResponder(StreamHandler):
    class StreamSubscriber(Subscriber):
        def __init__(self, stream: int, socket):
            super().__init__()
            self.stream = stream
            self.socket = socket

        def on_next(self, value: Payload, is_complete=False):
            self.socket.send_payload(
                self.stream, value, complete=is_complete)

        def on_complete(self):
            self.socket.send_payload(
                self.stream, Payload(b'', b''), complete=True)

            self.socket.finish_stream(self.stream)

        def on_error(self, exception: Exception):
            self.socket.send_error(self.stream, exception)
            self.socket.finish_stream(self.stream)

        def on_subscribe(self, subscription: Subscription):
            # noinspection PyAttributeOutsideInit
            self.subscription = subscription

    def __init__(self, stream: int, socket, publisher: Publisher):
        super().__init__(stream, socket)
        self.publisher = publisher
        self.subscriber = self.StreamSubscriber(stream, socket)
        self.publisher.subscribe(self.subscriber)

    async def frame_received(self, frame: Frame):
        if isinstance(frame, RequestStreamFrame):
            self.subscriber.subscription.request(frame.initial_request_n)
        elif isinstance(frame, CancelFrame):
            self.subscriber.subscription.cancel()
        elif isinstance(frame, RequestNFrame):
            self.subscriber.subscription.request(frame.request_n)
