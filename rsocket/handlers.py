from abc import abstractmethod, ABCMeta
from asyncio import Future, ensure_future
from typing import Union

from reactivestreams.publisher import Publisher
from reactivestreams.subscriber import Subscriber
from reactivestreams.subscription import Subscription
from rsocket.frame import CancelFrame, ErrorFrame, RequestNFrame, \
    RequestResponseFrame, RequestStreamFrame, PayloadFrame, Frame, RequestChannelFrame
from rsocket.payload import Payload


class StreamHandler(metaclass=ABCMeta):
    def __init__(self, stream: int, socket):
        super().__init__()
        self.stream = stream
        self.socket = socket

    def frame_sent(self, frame: Frame):
        """Not being marked abstract, since most handlers won't override."""

    @abstractmethod
    async def frame_received(self, frame: Frame):
        ...

    def send_cancel(self):
        """Convenience method for use by requester subclasses."""
        frame = CancelFrame()
        frame.stream_id = self.stream
        self.socket.send_frame(frame)
        self.socket.finish_stream(self.stream)

    def send_request_n(self, n: int):
        """Convenience method for use by requester subclasses."""
        frame = RequestNFrame()
        frame.stream_id = self.stream
        frame.request_n = n
        self.socket.send_frame(frame)


class RequestResponseRequester(StreamHandler, Future):
    def __init__(self, stream: int, socket, payload: Payload):
        super().__init__(stream, socket)
        request = RequestResponseFrame()
        request.stream_id = self.stream
        request.data = payload.data
        request.metadata = payload.metadata
        self.socket.send_frame(request)

    async def frame_received(self, frame: Frame):
        if isinstance(frame, PayloadFrame):
            self.set_result(Payload(frame.data, frame.metadata))
            self.socket.finish_stream(self.stream)
        elif isinstance(frame, ErrorFrame):
            self.set_exception(RuntimeError(frame.data))
            self.socket.finish_stream(self.stream)

    def cancel(self, *args, **kwargs):
        super().cancel(*args, **kwargs)
        self.send_cancel()


class RequestResponseResponder(StreamHandler):
    def __init__(self, stream: int, socket, future: Future):
        super().__init__(stream, socket)
        self.future = future
        self.future.add_done_callback(self.future_done)

    def future_done(self, future):
        if self.future.cancelled():
            pass
        elif not future.exception():
            ensure_future(self.socket.send_response(
                self.stream, future.result(), complete=True))
        else:
            ensure_future(self.socket.send_error(
                self.stream, future.exception()))
        self.socket.finish_stream(self.stream)

    async def frame_received(self, frame: Frame):
        if isinstance(frame, CancelFrame):
            self.future.cancel()


class RequestStreamRequester(StreamHandler, Publisher, Subscription):
    def __init__(self, stream: int, socket, payload: Payload):
        super().__init__(stream, socket)
        self.payload = payload

    def subscribe(self, subscriber):
        # noinspection PyAttributeOutsideInit
        self.subscriber = subscriber

        request = RequestStreamFrame()
        request.initial_request_n = 1
        request.stream_id = self.stream
        request.data = self.payload.data
        request.metadata = self.payload.metadata
        self.socket.send_frame(request)

        self.subscriber.on_subscribe(self)

    def cancel(self):
        super().cancel()
        self.send_cancel()

    async def request(self, n: int):
        self.send_request_n(n)

    async def frame_received(self, frame: Frame):
        if isinstance(frame, PayloadFrame):
            if frame.flags_next:
                self.subscriber.on_next(Payload(frame.data, frame.metadata))
            if frame.flags_complete:
                self.subscriber.on_complete()
                self.socket.finish_stream(self.stream)
        elif isinstance(frame, ErrorFrame):
            self.subscriber.on_error(RuntimeError(frame.data))
            self.socket.finish_stream(self.stream)


class RequestStreamResponder(StreamHandler):
    class StreamSubscriber(Subscriber):
        def __init__(self, stream: int, socket):
            super().__init__()
            self.stream = stream
            self.socket = socket

        def on_next(self, value, is_complete=False):
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


class RequestChannelRequesterResponder(StreamHandler, Publisher, Subscription):
    class StreamSubscriber(Subscriber):
        def __init__(self, stream: int, socket):
            super().__init__()
            self.stream = stream
            self.socket = socket

        def on_next(self, value, is_complete=False):
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

    def __init__(self, stream: int, socket, channel: Union[Publisher, Subscription, Subscriber]):
        super().__init__(stream, socket)
        self.channel = channel
        self.subscriber = self.StreamSubscriber(stream, socket)
        self.channel.subscribe(self.subscriber)
        self.subscribe(channel)

    async def frame_received(self, frame: Frame):
        if isinstance(frame, RequestChannelFrame):
            await self.channel.request(frame.initial_request_n)

        elif isinstance(frame, CancelFrame):
            self.channel.cancel()
        elif isinstance(frame, RequestNFrame):
            await self.channel.request(frame.request_n)

        elif isinstance(frame, PayloadFrame):
            if frame.flags_next:
                self.channel.on_next(Payload(frame.data, frame.metadata))
            if frame.flags_complete:
                self.channel.on_complete()
                self.socket.finish_stream(self.stream)
        elif isinstance(frame, ErrorFrame):
            self.channel.on_error(RuntimeError(frame.data))
            self.socket.finish_stream(self.stream)

    def subscribe(self, subscriber):
        self.subscriber = subscriber
        self.subscriber.on_subscribe(self)

    def cancel(self):
        super().cancel()
        self.send_cancel()

    async def request(self, n: int):
        self.send_request_n(n)
