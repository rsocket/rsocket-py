import asyncio
import functools
from typing import Optional

import rx
from rx import Observable
from rx.core import Observer
from rx.core.notification import OnNext, OnError
from rx.disposable import Disposable
from rx.operators import materialize
from rx.subject import Subject

from reactivestreams.publisher import Publisher
from reactivestreams.subscriber import Subscriber
from reactivestreams.subscription import Subscription
from rsocket.logger import logger
from rsocket.rx_support.subscriber_adapter import SubscriberAdapter


async def observable_to_async_generator(observable: Observable):
    queue = asyncio.Queue()

    def on_next(i):
        queue.put_nowait(i)

    disposable = observable.pipe(materialize()).subscribe(
        on_next=on_next
    )

    while True:
        value = await queue.get()
        if isinstance(value, OnNext):
            yield value.value
            queue.task_done()
        elif isinstance(value, OnError):
            disposable.dispose()
            raise (Exception(value.value))
        else:
            disposable.dispose()
            break


def from_aiter(iterator, feedback: Optional[Observable] = None):
    loop = asyncio.get_event_loop()

    def on_subscribe(observer: Observer, scheduler):
        async def _aio_sub():
            try:
                async for i in iterator:
                    observer.on_next(i)
                loop.call_soon(observer.on_completed)
            except Exception as e:
                loop.call_soon(functools.partial(
                    observer.on_error, e))

        async def _aio_next():
            try:
                i = await iterator.__anext__()
                observer.on_next(i)
            except StopAsyncIteration:
                observer.on_completed()
            except Exception as e:
                logger().error(str(e), exc_info=True)
                observer.on_error(e)

        if feedback is not None:
            return feedback.subscribe(
                on_next=lambda i: asyncio.ensure_future(_aio_next())
            )
        else:
            task = asyncio.ensure_future(_aio_sub())
            return Disposable(lambda: task.cancel())

    return rx.create(on_subscribe)


class BackPressurePublisher(Publisher, Subscription):
    def __init__(self, wrapped_observable: Observable):
        self._wrapped_observable = wrapped_observable

    def subscribe(self, subscriber: Subscriber):
        subscriber.on_subscribe(self)
        self._feedback = Subject()
        async_iterator = observable_to_async_generator(self._wrapped_observable).__aiter__()
        self._subscriber = subscriber
        from_aiter(async_iterator, self._feedback).subscribe(SubscriberAdapter(subscriber))

    def request(self, n: int):
        for i in range(n):
            self._feedback.on_next(True)

    def cancel(self):
        self._feedback.on_completed()
