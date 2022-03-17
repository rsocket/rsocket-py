import asyncio
from typing import Optional

import rx
from rx import Observable
from rx.core import Observer
from rx.core.notification import OnNext, OnError, OnCompleted
from rx.operators import materialize
from rx.subject import Subject

from reactivestreams.subscriber import Subscriber
from rsocket.helpers import DefaultPublisherSubscription
from rsocket.logger import logger
from rsocket.rx_support.subscriber_adapter import SubscriberAdapter


async def observable_to_async_event_generator(observable: Observable):
    queue = asyncio.Queue()

    def on_next(i):
        queue.put_nowait(i)

    observable.pipe(materialize()).subscribe(
        on_next=on_next
    )

    while True:
        value = await queue.get()
        yield value
        queue.task_done()


def from_aiter(iterator, feedback: Optional[Observable] = None):
    # noinspection PyUnusedLocal
    def on_subscribe(observer: Observer, scheduler):
        async def _aio_next():
            try:
                event = await iterator.__anext__()

                if isinstance(event, OnNext):
                    observer.on_next(event.value)
                elif isinstance(event, OnError):
                    observer.on_error(event.exception)
                elif isinstance(event, OnCompleted):
                    observer.on_completed()
            except StopAsyncIteration:
                pass
            except Exception as exception:
                logger().error(str(exception), exc_info=True)
                observer.on_error(exception)

        def create_next_task():
            asyncio.create_task(_aio_next())

        return feedback.subscribe(
            on_next=lambda i: create_next_task()
        )

    return rx.create(on_subscribe)


class BackPressurePublisher(DefaultPublisherSubscription):
    def __init__(self, wrapped_observable: Observable):
        self._wrapped_observable = wrapped_observable
        self._feedback = None

    def subscribe(self, subscriber: Subscriber):
        super().subscribe(subscriber)
        self._feedback = Subject()
        async_iterator = observable_to_async_event_generator(self._wrapped_observable).__aiter__()
        from_aiter(async_iterator, self._feedback).subscribe(SubscriberAdapter(subscriber))

    def request(self, n: int):
        for i in range(n):
            self._feedback.on_next(True)

    def cancel(self):
        self._feedback.on_completed()
