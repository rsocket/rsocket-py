import asyncio
import logging
from asyncio import Task
from typing import Optional

from rx.core import Observer

from reactivestreams.subscriber import Subscriber
from reactivestreams.subscription import Subscription
from rsocket.payload import Payload


def logger(): return logging.getLogger(__name__)


class BackPressureSubscriber(Subscriber):

    def __init__(self,
                 wrapped_observer: Observer,
                 request_count: int):
        self.wrapped_observer = wrapped_observer
        self._request_count = request_count

        self._message_queue = asyncio.Queue(request_count)
        self._messages_received = 0
        self._messages_processed = 0

        self._processing_task: Optional[Task] = None

        self._is_completed = False
        self._error: Optional[Exception] = None

    def on_subscribe(self, subscription: Subscription):
        self._subscription = subscription

    def _request_next_n_messages(self):
        self._subscription.request(self._request_count)
        self._start_processing_messages()

    def _start_processing_messages(self):
        self._processing_task = asyncio.create_task(self._process_message_batch())

    def on_complete(self):
        self._is_completed = True

    def on_next(self, value: Payload, is_complete=False):
        self._messages_received += 1
        self._message_queue.put_nowait((value, is_complete))

        if self._processing_task is None:
            self._start_processing_messages()

    async def _process_message_batch(self):
        while True:
            if self._message_queue.empty() and self._error:
                self.wrapped_observer.on_error(self._error)
                self._processing_task.cancel()
                break

            if self._message_queue.empty() and self._is_completed:
                self.wrapped_observer.on_completed()
                self._processing_task.cancel()
                break

            payload, complete = await self._message_queue.get()

            self.wrapped_observer.on_next(payload)
            self._messages_processed += 1

            if complete:
                self._is_completed = complete
                self.wrapped_observer.on_completed()
                break

            if self._messages_processed == self._request_count:
                self._messages_processed = 0
                self._request_next_n_messages()
                break

        if not self._is_completed and self._error is None:
            self._request_next_n_messages()

    def on_error(self, error: Exception):
        self._error = error
