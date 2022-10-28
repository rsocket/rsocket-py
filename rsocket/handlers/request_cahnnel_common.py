import abc
import asyncio
from typing import Optional

from reactivestreams.publisher import Publisher
from reactivestreams.subscriber import Subscriber, DefaultSubscriber
from reactivestreams.subscription import Subscription
from rsocket.frame import CancelFrame, ErrorFrame, RequestNFrame, \
    PayloadFrame, Frame, error_frame_to_exception
from rsocket.helpers import payload_from_frame
from rsocket.logger import logger
from rsocket.payload import Payload
from rsocket.rsocket import RSocket
from rsocket.streams.stream_handler import StreamHandler


class StreamSubscriber(DefaultSubscriber):
    def __init__(self, stream_id: int, socket, requester: 'RequestChannelCommon'):
        super().__init__()
        self._stream_id = stream_id
        self._socket = socket
        self._requester = requester

    def on_next(self, value, is_complete=False):
        self._socket.send_payload(
            self._stream_id, value, complete=is_complete)

        if is_complete:
            self._requester.mark_completed_and_finish(sent=True)

    def on_complete(self):
        self._socket.send_payload(
            self._stream_id, Payload(), complete=True, is_next=False)
        self._requester.mark_completed_and_finish(sent=True)

    def on_error(self, exception):
        self._socket.send_error(self._stream_id, exception)
        self._requester.mark_completed_and_finish(sent=True)


class RequestChannelCommon(StreamHandler, Publisher, Subscription, metaclass=abc.ABCMeta):

    def __init__(self,
                 socket: RSocket,
                 remote_publisher: Optional[Publisher] = None,
                 sending_done_event: Optional[asyncio.Event] = None):
        super().__init__(socket)
        self._sending_done_event = sending_done_event
        self.remote_subscriber = None
        self._sent_complete = False
        self._received_complete = False
        self._remote_publisher = remote_publisher
        self.subscriber = None

    def setup(self):
        self.subscriber = StreamSubscriber(self.stream_id, self.socket, self)

        if self._remote_publisher is not None:
            self._remote_publisher.subscribe(self.subscriber)

    def frame_received(self, frame: Frame):
        if isinstance(frame, CancelFrame):
            self.subscriber.subscription.cancel()
            self.mark_completed_and_finish(sent=True)
        elif isinstance(frame, RequestNFrame):
            if self.subscriber.subscription is not None:
                self.subscriber.subscription.request(frame.request_n)
            else:
                logger().warning('%s: Received request_n but no publisher provided', self.__class__.__name__)

        elif isinstance(frame, PayloadFrame):
            if frame.flags_next:
                self.remote_subscriber.on_next(payload_from_frame(frame),
                                               is_complete=frame.flags_complete)
            elif frame.flags_complete:
                self.remote_subscriber.on_complete()

            if frame.flags_complete:
                self.mark_completed_and_finish(received=True)
        elif isinstance(frame, ErrorFrame):
            self.remote_subscriber.on_error(error_frame_to_exception(frame))
            self.mark_completed_and_finish(received=True)

    def _complete_remote_subscriber(self):
        if self.remote_subscriber is not None:
            self.remote_subscriber.on_complete()

        self.mark_completed_and_finish(received=True)

    def mark_completed_and_finish(self, received=None, sent=None):

        if received:
            self._received_complete = True

        if sent:
            self._sent_complete = True
            self._set_sending_done()

        self._finish_if_both_closed()

    def _finish_if_both_closed(self):
        if self._received_complete and self._sent_complete:
            self._finish_stream()

    def subscribe(self, subscriber: Optional[Subscriber]):
        if subscriber is not None:
            self.remote_subscriber = subscriber
            self.remote_subscriber.on_subscribe(self)
        else:
            self.mark_completed_and_finish(received=True)

    def cancel(self):
        self.send_cancel()
        self._set_sending_done()

    def _set_sending_done(self):
        if self._sending_done_event is not None:
            self._sending_done_event.set()

    def request(self, n: int):
        self.send_request_n(n)
