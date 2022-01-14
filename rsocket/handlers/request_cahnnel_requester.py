from asyncio import ensure_future

from reactivestreams.publisher import Publisher
from reactivestreams.subscriber import Subscriber
from reactivestreams.subscription import Subscription
from rsocket.frame import CancelFrame, ErrorFrame, RequestNFrame, \
    PayloadFrame, Frame, RequestChannelFrame
from rsocket.handlers.stream_handler import StreamHandler
from rsocket.payload import Payload


class RequestChannelRequester(StreamHandler, Publisher, Subscription):
    class StreamSubscriber(Subscriber):
        def __init__(self, stream: int, socket,
                     requester: 'RequestChannelRequester'):
            super().__init__()
            self._stream = stream
            self._socket = socket
            self._requester = requester

        async def on_next(self, value, is_complete=False):
            ensure_future(self._socket.send_response(
                self._stream, value, complete=is_complete))

        def on_complete(self, value=None):
            if value is None:
                value = Payload(b'', b'')

            ensure_future(self._socket.send_response(
                self._stream, value, complete=True))
            self._requester._sent_complete = True
            self._requester._finish_if_both_closed()

        def on_error(self, exception):
            ensure_future(self._socket.send_error(self._stream, exception))
            self._requester._sent_complete = True
            self._requester._finish_if_both_closed()

        def on_subscribe(self, subscription):
            # noinspection PyAttributeOutsideInit
            self.subscription = subscription

    def __init__(self, stream: int, socket, local_publisher: Publisher):
        super().__init__(stream, socket)
        self.subscriber = self.StreamSubscriber(stream, socket, self)
        self.local_publisher = local_publisher
        local_publisher.subscribe(self.subscriber)

        self._sent_complete = False
        self._received_complete = False

    async def frame_received(self, frame: Frame):
        if isinstance(frame, CancelFrame):
            self.subscriber.subscription.cancel()
        elif isinstance(frame, RequestNFrame):
            await self.subscriber.subscription.request(frame.request_n)

        elif isinstance(frame, PayloadFrame):
            if frame.flags_next:
                await self.local_subscriber.on_next(Payload(frame.data, frame.metadata))
            if frame.flags_complete:
                self.local_subscriber.on_complete()
                self._received_complete = True
                self._finish_if_both_closed()
        elif isinstance(frame, ErrorFrame):
            self.local_subscriber.on_error(RuntimeError(frame.data))
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

    async def request(self, n: int):
        self.send_request_n(n)

    def send_channel_request(self, payload: Payload):
        request = RequestChannelFrame()
        request.initial_request_n = self._initial_request_n
        request.stream_id = self.stream
        request.data = payload.data
        request.metadata = payload.metadata
        self.socket.send_frame(request)
