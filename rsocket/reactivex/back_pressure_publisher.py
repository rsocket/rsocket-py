import asyncio
from typing import Optional

import reactivex
from reactivex import Observable, Observer
from reactivex.notification import OnNext, OnError, OnCompleted
from reactivex.operators import materialize
from reactivex.subject import Subject

from reactivestreams.subscriber import Subscriber
from rsocket.helpers import DefaultPublisherSubscription
from rsocket.logger import logger
from rsocket.reactivex.subscriber_adapter import SubscriberAdapter


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

        request_n_queue = asyncio.Queue()

        async def _aio_next():

            try:
                while True:
                    next_n = await request_n_queue.get()
                    for i in range(next_n):
                        event = await iterator.__anext__()

                        if isinstance(event, OnNext):
                            observer.on_next(event.value)
                        elif isinstance(event, OnError):
                            observer.on_error(event.exception)
                            return
                        elif isinstance(event, OnCompleted):
                            observer.on_completed()
                            return
            except StopAsyncIteration:
                return
            except Exception as exception:
                logger().error(str(exception), exc_info=True)
                observer.on_error(exception)

        sender = asyncio.create_task(_aio_next())

        def cancel_sender():
            sender.cancel()

        return feedback.subscribe(
            on_next=lambda n: request_n_queue.put_nowait(n),
            on_completed=cancel_sender
        )

    return reactivex.create(on_subscribe)


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
        self._feedback.on_next(n)

    def cancel(self):
        self._feedback.on_completed()
