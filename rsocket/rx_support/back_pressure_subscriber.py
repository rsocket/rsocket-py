import asyncio
import logging
from asyncio import Task
from enum import Enum, unique, auto
from typing import Optional

from rx.core import Observer

from reactivestreams.subscriber import Subscriber
from reactivestreams.subscription import Subscription
from rsocket.payload import Payload


def logger(): return logging.getLogger(__name__)


@unique
class EventType(Enum):
    NEXT = auto()
    ERROR = auto()
    COMPLETE = auto()


class BackPressureSubscriber(Subscriber):

    def __init__(self,
                 wrapped_observer: Observer,
                 request_count: int):
        self.wrapped_observer = wrapped_observer
        self._request_count = request_count

        self._message_queue = asyncio.Queue()
        self._messages_received = 0
        self._messages_processed = 0

        self._processing_task: Optional[Task] = None

        self._is_completed = False

    def on_subscribe(self, subscription: Subscription):
        self._subscription = subscription

    def _request_next_n_messages(self):
        self._subscription.request(self._request_count)
        self._start_processing_messages()

    def _start_processing_messages(self):
        self._processing_task = asyncio.create_task(self._process_message_batch())

    def on_complete(self):
        if self._processing_task is None:
            self.wrapped_observer.on_completed()
            self._cancel_processing_task()
        else:
            self._message_queue.put_nowait((EventType.COMPLETE, None, True))

    def on_next(self, value: Payload, is_complete=False):
        self._messages_received += 1
        self._message_queue.put_nowait((EventType.NEXT, value, is_complete))

        if self._processing_task is None:
            self._start_processing_messages()

    async def _process_message_batch(self):
        try:
            while True:
                event_type, payload, complete = await self._message_queue.get()
                if event_type == EventType.NEXT:
                    self.wrapped_observer.on_next(payload)
                    self._messages_processed += 1

                if complete:
                    self._is_completed = complete
                    self.wrapped_observer.on_completed()
                    self._processing_task.cancel()
                    return

                if event_type == EventType.ERROR:
                    self.wrapped_observer.on_error(payload)
                    self._processing_task.cancel()
                    return

                if self._messages_processed == self._request_count:
                    self._messages_processed = 0
                    self._request_next_n_messages()
                    break

            if not self._is_completed:
                self._request_next_n_messages()
        except asyncio.CancelledError:
            pass

    def on_error(self, error: Exception):
        if self._processing_task is None:
            self.wrapped_observer.on_error(error)
            self._cancel_processing_task()
        else:
            self._message_queue.put_nowait((EventType.ERROR, error, None))

    def _cancel_processing_task(self):
        if self._processing_task is not None:
            self._processing_task.cancel()
