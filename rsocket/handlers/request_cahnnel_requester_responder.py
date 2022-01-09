from asyncio import ensure_future
from typing import Union

from reactivestreams.publisher import Publisher
from reactivestreams.subscriber import Subscriber
from reactivestreams.subscription import Subscription
from rsocket.frame import CancelFrame, ErrorFrame, RequestNFrame, \
    PayloadFrame, Frame, RequestChannelFrame
from rsocket.handlers.stream_handler import StreamHandler
from rsocket.payload import Payload


class RequestChannelRequesterResponder(StreamHandler, Publisher, Subscription):
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
                await self.channel.on_next(Payload(frame.data, frame.metadata))
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
        self.send_cancel()

    async def request(self, n: int):
        self.send_request_n(n)

    def send_channel_request(self, payload: Payload):
        request = RequestChannelFrame()
        request.initial_request_n = self._initial_request_n
        request.stream_id = self.stream
        request.data = payload.data
        request.metadata = payload.metadata
        self.socket.send_frame(request)
