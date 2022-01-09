from asyncio import ensure_future

from reactivestreams.publisher import Publisher
from reactivestreams.subscriber import Subscriber
from rsocket.frame import CancelFrame, RequestNFrame, \
    RequestStreamFrame, Frame
from rsocket.handlers.stream_handler import StreamHandler
from rsocket.payload import Payload


class RequestStreamResponder(StreamHandler):
    class StreamSubscriber(Subscriber):
        def __init__(self, stream: int, socket):
            super().__init__()
            self.stream = stream
            self.socket = socket

        async def on_next(self, value, is_complete=False):
            ensure_future(self.socket.send_response(
                self.stream, value, complete=is_complete))

        def on_complete(self, value=None):
            if value is None:
                value = Payload(b'', b'')

            ensure_future(self.socket.send_response(
                self.stream, value, complete=True))

            self.socket.finish_stream(self.stream)

        def on_error(self, exception):
            ensure_future(self.socket.send_error(self.stream, exception))
            self.socket.finish_stream(self.stream)

        def on_subscribe(self, subscription):
            # noinspection PyAttributeOutsideInit
            self.subscription = subscription

    def __init__(self, stream: int, socket, publisher: Publisher):
        super().__init__(stream, socket)
        self.publisher = publisher
        self.subscriber = self.StreamSubscriber(stream, socket)
        self.publisher.subscribe(self.subscriber)

    async def frame_received(self, frame: Frame):
        if isinstance(frame, RequestStreamFrame):
            await self.subscriber.subscription.request(frame.initial_request_n)
        elif isinstance(frame, CancelFrame):
            self.subscriber.subscription.cancel()
        elif isinstance(frame, RequestNFrame):
            await self.subscriber.subscription.request(frame.request_n)
