import asyncio

from rxbp.acknowledgement.acksubject import AckSubject
from rxbp.acknowledgement.continueack import continue_ack
from rxbp.acknowledgement.stopack import stop_ack
from rxbp.typing import ElementType

from reactivestreams import Publisher, Subscriber, Subscription
from rsocket.acksubscriber import AsyncIOAckSubscriber, MAX_REQUEST_N
from rsocket.frame import RequestChannelFrame
from rsocket.payload import Payload


class SinkSubscriber(Publisher):
    def __init__(self):
        self.is_subscribed = False
        self.is_done = False

    def subscribe(self, subscriber: Subscriber):
        # noinspection PyAttributeOutsideInit
        self.subscriber = subscriber
        self.is_subscribed = True

    def handler_on_subscribe(self, sub: Subscription):
        if self.is_subscribed:
            self.subscriber.on_subscribe(sub)

    def next(self, value: ElementType):
        if self.is_subscribed:
            self.subscriber.on_next(value)

    def error(self, exc: Exception):
        if self.is_subscribed:
            self.subscriber.on_error(exc)

    def completed(self):
        if self.is_subscribed:
            self.subscriber.on_completed()


class RequestChannelRequestSubscriber(AsyncIOAckSubscriber):
    __slots__ = 'initial_request_n'

    def __init__(
            self,
            stream: int,
            socket,
            loop: asyncio.AbstractEventLoop,
            initial_request_n: int = MAX_REQUEST_N
    ):
        super().__init__(stream, socket, loop)

        self.is_first_payload = True
        self.initial_request_n = initial_request_n

    def on_next(self, payloads: ElementType):
        try:
            payloads = list(payloads)
        except Exception as e:
            self.socket.finish_stream(self.stream)
            self.on_error(e)
            # return stop_ack
            raise e

        for payload in payloads:
            if self.is_done():
                return stop_ack

            if self.is_first_payload:
                self._send_first_payload(payload)
            else:
                asyncio.run_coroutine_threadsafe(
                    self.socket.send_response(self.stream, payload, flags_next=True),
                    self.loop)

        with self.lock:
            self.on_next_counter += 1
            on_next_counter = self.on_next_counter
            request_n = self.request_n

        if on_next_counter < request_n:
            return_ack = continue_ack
        else:
            return_ack = AckSubject()

        self.last_ack = return_ack
        return return_ack

    def _send_first_payload(self, payload: Payload):
        request = RequestChannelFrame()
        request.initial_request_n = self.initial_request_n
        request.stream_id = self.stream
        request.data = payload.data
        request.metadata = payload.metadata

        async def _send_frame():
            self.socket.send_frame(request)

        asyncio.run_coroutine_threadsafe(_send_frame(), self.loop)

        self.is_first_payload = False

    def on_completed(self):
        super().on_completed()
        self._completed()

    def _completed(self):
        if self.subscription.receive_completed:
            self.socket.finish_stream(self.stream)


class RequestChannelRespondSubscriber(AsyncIOAckSubscriber):
    def __init__(
            self,
            stream: int,
            socket,
            loop: asyncio.AbstractEventLoop,
            initial_request_n: int
    ):
        super().__init__(stream, socket, loop, initial_request_n)

    def on_completed(self):
        super().on_completed()
        self._completed()

    def _completed(self):
        if self.subscription.receive_completed:
            self.socket.finish_stream(self.stream)
