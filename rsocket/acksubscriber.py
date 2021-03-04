import asyncio
import threading

from rxbp.acknowledgement.ack import Ack
from rxbp.acknowledgement.acksubject import AckSubject
from rxbp.acknowledgement.continueack import continue_ack
from rxbp.acknowledgement.stopack import stop_ack
from rxbp.observer import Observer
from rxbp.typing import ElementType

from rsocket.payload import Payload

MAX_REQUEST_N = 0x7FFFFFFF


class AsyncIOAckSubscriber(Observer):
    def __init__(self, stream: int, socket, loop: asyncio.AbstractEventLoop, request_n: int = 1):
        self.stream = stream
        self.socket = socket
        self.loop = loop
        self.request_n = request_n

        self.lock = threading.RLock()

        self.last_ack = None
        self.is_stopped = False
        self.is_completed = False
        self.on_next_counter = 0

    def on_next(self, payloads: ElementType) -> Ack:
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

    def on_error(self, exception):
        if self.is_done():
            return

        asyncio.run_coroutine_threadsafe(
            self.socket.send_error(self.stream, exception),
            self.loop)
        self.socket.finish_stream(self.stream)

    def on_completed(self):
        if self.is_done():
            return

        asyncio.run_coroutine_threadsafe(
            self.socket.send_response(self.stream, Payload(b'', b''), complete=True),
            self.loop)
        self.is_completed = True

    def incr_request_n(self, num: int):
        with self.lock:
            self.request_n += num
            on_next_counter = self.on_next_counter
            request_n = self.request_n

        if on_next_counter < request_n:
            if isinstance(self.last_ack, AckSubject):
                self.last_ack.on_next(continue_ack)

    def dispose(self):
        self.is_stopped = True

    def is_done(self):
        return self.is_stopped or self.is_completed

    def on_subscribe(self, subscription):
        # noinspection PyAttributeOutsideInit
        self.subscription = subscription
