import asyncio
from abc import abstractmethod, ABCMeta
from asyncio import Future, ensure_future

from rxbp.flowable import Flowable

from reactivestreams import Publisher, Subscription, Subscriber
from reactivestreams.subscriber import DefaultSubscriber
from rsocket.acksubscriber import MAX_REQUEST_N
from rsocket.frame import CancelFrame, ErrorFrame, RequestNFrame, \
    RequestResponseFrame, RequestStreamFrame, PayloadFrame
from rsocket.payload import Payload
from rsocket.subscriberrequestchannel import SinkSubscriber, RequestChannelRespondSubscriber, \
    RequestChannelRequestSubscriber
from rsocket.subscriberrequeststream import StreamSubscriber


class StreamHandler(metaclass=ABCMeta):
    def __init__(self, stream: int, socket):
        super().__init__()
        self.stream = stream
        self.socket = socket

    # Not being marked abstract, since most handlers won't override.
    def frame_sent(self, frame):
        pass

    @abstractmethod
    def frame_received(self, frame):
        pass

    def send_cancel(self):
        """Convenience method for use by requester subclasses."""
        frame = CancelFrame()
        frame.stream_id = self.stream
        self.socket.send_frame(frame)
        self.socket.finish_stream(self.stream)

    def send_request_n(self, n):
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

    def frame_received(self, frame):
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

    def frame_received(self, frame):
        if isinstance(frame, CancelFrame):
            self.future.cancel()


class RequestStreamRequester(StreamHandler, Publisher, Subscription):
    def __init__(self, stream: int, socket, payload: Payload):
        super().__init__(stream, socket)

        self.payload = payload
        self.has_request = False
        self.initial_request_n = MAX_REQUEST_N

    def subscribe(self, subscriber):
        # noinspection PyAttributeOutsideInit
        self.subscriber = subscriber
        self.subscriber.on_subscribe(self)

        request = RequestStreamFrame()
        request.initial_request_n = self.initial_request_n
        request.stream_id = self.stream
        request.data = self.payload.data
        request.metadata = self.payload.metadata
        self.socket.send_frame(request)
        self.has_request = True

    def cancel(self, *args, **kwargs):
        self.send_cancel()

    def request(self, n):
        if not 0 < n < MAX_REQUEST_N:
            return

        if not self.has_request:
            self.initial_request_n = self._add_request_n(n)
        else:
            self.send_request_n(n)

    def _add_request_n(self, n):
        if self.initial_request_n == MAX_REQUEST_N and not self.has_request:
            return n

        res = self.initial_request_n + n

        if not 0 < res < MAX_REQUEST_N:
            return MAX_REQUEST_N

        return res

    def frame_received(self, frame):
        if isinstance(frame, PayloadFrame):
            if frame.data or not frame.flags_complete:
                self.subscriber.on_next([Payload(frame.data, frame.metadata)])
            if frame.flags_complete:
                self.subscriber.on_completed()
                self.socket.finish_stream(self.stream)
        elif isinstance(frame, ErrorFrame):
            self.subscriber.on_error(RuntimeError(frame.data))
            self.socket.finish_stream(self.stream)


class RequestStreamResponder(StreamHandler):
    def __init__(self, stream: int, socket, publisher: Flowable, initial_request_n: int):
        super().__init__(stream, socket)

        self.publisher = publisher
        self.sink = StreamSubscriber(
            stream, socket, asyncio.get_event_loop(),
            initial_request_n
        )
        self.sink.on_subscribe(self)
        self.subscription = self.publisher.subscribe(observer=self.sink)

    def frame_received(self, frame):
        if isinstance(frame, CancelFrame):
            self.sink.dispose()
            self.subscription.dispose()
            self.socket.finish_stream(self.stream)
        elif isinstance(frame, RequestNFrame):
            self.sink.incr_request_n(frame.request_n[0])


class RequestChannelRequester(StreamHandler, Publisher, Subscription):
    def __init__(self, stream: int, socket, publisher: Flowable):
        super().__init__(stream, socket)
        self.sending = publisher
        self.is_subscribed = False
        self.receive_completed = False
        self.initial_request_n = MAX_REQUEST_N
        self.sink = RequestChannelRequestSubscriber(self.stream, self.socket, asyncio.get_event_loop())
        self.sink.on_subscribe(self)

        self.receiving: Subscriber = DefaultSubscriber()

    def frame_received(self, frame):
        if isinstance(frame, ErrorFrame):
            self._dispose()
            self.receiving.on_error(RuntimeError(frame.data))
            self.socket.finish_stream(self.stream)
        elif isinstance(frame, PayloadFrame):
            if frame.data or not frame.flags_complete:
                self.receiving.on_next([Payload(frame.data, frame.metadata)])
            if frame.flags_complete:
                self.receiving.on_completed()
                self.receive_completed = True
                if self.sink.is_completed:
                    self.socket.finish_stream(self.stream)
        elif isinstance(frame, RequestNFrame):
            self.sink.incr_request_n(frame.request_n[0])
        elif isinstance(frame, CancelFrame):
            self._dispose()

    def subscribe(self, subscriber):
        if self.is_subscribed:
            return

        self.receiving = subscriber
        self.receiving.on_subscribe(self)

        self.sink.initial_request_n = self.initial_request_n
        # noinspection PyAttributeOutsideInit
        self.sending_subscription = self.sending.subscribe(observer=self.sink)
        self.is_subscribed = True

    def request(self, n):
        if not 0 < n < MAX_REQUEST_N:
            return

        if self.sink.is_first_payload:
            self.initial_request_n = self._add_request_n(n)
        else:
            self.send_request_n(n)

    def cancel(self, *args, **kwargs):
        self.send_cancel()

    def _add_request_n(self, n):
        if self.initial_request_n == MAX_REQUEST_N and self.sink.is_first_payload:
            return n

        res = self.initial_request_n + n

        if not 0 < res < MAX_REQUEST_N:
            return MAX_REQUEST_N

        return res

    def _dispose(self):
        self.sink.dispose()
        self.sending_subscription.dispose()


class RequestChannelResponder(StreamHandler, Subscription):
    def __init__(
            self,
            stream: int,
            socket,
            initial_request_n: int,
            receiving: SinkSubscriber,
            sending: Flowable
    ):
        super().__init__(stream, socket)

        self.receive_completed = False

        self.sending = sending
        self.sink = RequestChannelRespondSubscriber(
            stream, socket, asyncio.get_event_loop(),
            initial_request_n,
        )
        self.sink.on_subscribe(self)
        self.subscription = self.sending.subscribe(observer=self.sink)

        # TODO processor
        self.receiving = receiving
        self.receiving.handler_on_subscribe(self)

    def request(self, n):
        self.send_request_n(n)

    def cancel(self, *args, **kwargs):
        self.send_cancel()

    def frame_received(self, frame):
        if isinstance(frame, ErrorFrame):
            self._dispose()
            self.receiving.error(RuntimeError(frame.data))
            self.socket.finish_stream(self.stream)
        elif isinstance(frame, CancelFrame):
            self._dispose()
        elif isinstance(frame, PayloadFrame):
            if frame.data or not frame.flags_complete:
                self.receiving.next([Payload(frame.data, frame.metadata)])
            if frame.flags_complete:
                self.receiving.completed()
                self.receive_completed = True
                if self.sink.is_completed:
                    self.socket.finish_stream(self.stream)
        elif isinstance(frame, RequestNFrame):
            self.sink.incr_request_n(frame.request_n[0])

    def _dispose(self):
        self.sink.dispose()
        self.subscription.dispose()
