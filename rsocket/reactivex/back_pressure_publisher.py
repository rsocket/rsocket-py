import asyncio
from asyncio import Queue
from dataclasses import dataclass
from typing import Callable, AsyncGenerator, Union, Optional

import reactivex
from reactivex import Observable, Observer
from reactivex.notification import OnNext, OnError, OnCompleted, Notification
from reactivex.operators import materialize
from reactivex.subject import Subject

from reactivestreams.publisher import Publisher
from reactivestreams.subscriber import Subscriber
from rsocket.async_helpers import async_range
from rsocket.helpers import DefaultPublisherSubscription
from rsocket.logger import logger
from rsocket.reactivex.subscriber_adapter import SubscriberAdapter

__all__ = [
    'observable_to_publisher',
    'from_observable_with_backpressure',
    'observable_from_queue',
    'observable_from_async_generator'
]

from rsocket.streams.helpers import async_generator_from_queue


@dataclass(frozen=True)
class ObservableBackpressureFactory:
    factory: Callable[[Subject], Observable]

    def __call__(self, subject: Subject) -> Observable:
        return self.factory(subject)


def from_observable_with_backpressure(factory: Callable[[Subject], Observable]) -> Callable[[Subject], Observable]:
    return ObservableBackpressureFactory(factory)


def observable_from_queue(queue: Queue, backpressure: Subject):
    return observable_from_async_generator(
        async_generator_from_queue(queue),
        backpressure
    )


async def task_from_awaitable(future):
    async def coroutine_from_awaitable(awaitable):
        return await awaitable

    task = asyncio.create_task(coroutine_from_awaitable(future))
    await asyncio.sleep(0)  # allow awaitable to be accessed at least once
    return task


def observable_from_async_generator(iterator, backpressure: Subject) -> Observable:
    # noinspection PyUnusedLocal
    def on_subscribe(observer: Observer, scheduler):

        request_n_queue = asyncio.Queue()

        def request_next_n(n):
            request_n_queue.put_nowait(n)

        async def _aio_next():

            try:
                while True:
                    try:
                        next_n = await request_n_queue.get()
                    except RuntimeError:
                        return

                    async for i in async_range(next_n):
                        try:
                            value = await iterator.__anext__()
                            observer.on_next(value)
                        except StopAsyncIteration:
                            observer.on_completed()
                            return
                        except Exception as exception:
                            logger().error(str(exception), exc_info=True)
                            observer.on_error(exception)
                            return
            except Exception as exception:
                logger().error(str(exception), exc_info=True)
                observer.on_error(exception)

        def cancel_sender():
            sender.cancel()

        result = backpressure.subscribe(
            on_next=request_next_n,
            on_completed=cancel_sender
        )

        sender = asyncio.create_task(_aio_next())

        return result

    return reactivex.create(on_subscribe)


async def observable_to_async_event_generator(observable: Observable) -> AsyncGenerator[Notification, None]:
    queue = asyncio.Queue()

    completed = object()

    def on_next_event(event):
        queue.put_nowait(event)

    observable.pipe(materialize()).subscribe(
        on_next=on_next_event,
        on_completed=lambda: queue.put_nowait(completed)
    )

    while True:
        value = await queue.get()

        if value is completed:
            queue.task_done()
            return

        yield value
        queue.task_done()


def from_async_event_generator(generator: AsyncGenerator[Notification, None], backpressure: Subject) -> Observable:
    return from_async_event_iterator(generator.__aiter__(), backpressure)


def from_async_event_iterator(iterator, backpressure: Subject) -> Observable:
    # noinspection PyUnusedLocal
    def on_subscribe(observer: Observer, scheduler):

        request_n_queue = asyncio.Queue()

        async def _aio_next():

            try:
                while True:
                    next_n = await request_n_queue.get()
                    async for i in async_range(next_n):
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
                logger().error('Observable event stream failure')
                return
            except Exception as exception:
                logger().error(str(exception), exc_info=True)
                observer.on_error(exception)

        sender = asyncio.create_task(_aio_next())

        def cancel_sender():
            sender.cancel()

        return backpressure.subscribe(
            on_next=lambda n: request_n_queue.put_nowait(n),
            on_completed=cancel_sender
        )

    return reactivex.create(on_subscribe)


class InternalBackPressurePublisher(DefaultPublisherSubscription):
    def __init__(self, _observable_factory: Callable[[Subject], Observable]):
        self._factory = _observable_factory
        self._feedback: Optional[Subject] = None

    def subscribe(self, subscriber: Subscriber):
        super().subscribe(subscriber)
        self._feedback = Subject()
        observable = self._factory(self._feedback)
        observable.subscribe(SubscriberAdapter(subscriber))

    def request(self, n: int):
        self._feedback.on_next(n)

    def cancel(self):
        self._feedback.on_completed()


def feedback_observable(observable_factory: Callable[[Subject], Observable]):
    return InternalBackPressurePublisher(observable_factory)


def observable_to_publisher(
        observable: Optional[Union[Observable, Callable[[Subject], Observable]]]
) -> Optional[Publisher]:
    if observable is None:
        return observable

    if isinstance(observable, ObservableBackpressureFactory):
        return feedback_observable(observable)
    else:
        return BackPressurePublisher(observable)


class BackPressurePublisher(InternalBackPressurePublisher):
    def __init__(self, wrapped_observable: Observable):
        def local_factory(feedback: Subject) -> Observable:
            async_generator = observable_to_async_event_generator(wrapped_observable)
            return from_async_event_generator(async_generator, feedback)

        super().__init__(local_factory)
