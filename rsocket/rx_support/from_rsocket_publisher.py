import asyncio
import functools

import rx
from rx import Observable
from rx.core import Observer
from rx.disposable import Disposable

from reactivestreams.publisher import Publisher
from reactivestreams.subscriber import Subscriber
from reactivestreams.subscription import Subscription


def from_rsocket_publisher(publisher: Publisher, limit_rate=5) -> Observable:
    loop = asyncio.get_event_loop()

    def on_subscribe(observer: Observer, scheduler):
        done = asyncio.Event()
        get_next_n = None

        class RxSubscriber(Subscriber):
            def __init__(self):
                self.is_done = False
                self._received_messages = 0

            def on_subscribe(self, subscription: Subscription):
                self.subscription = subscription

            def on_next(self, value, is_complete=False):
                if self.is_done:
                    return

                self._received_messages += 1
                observer.on_next(value)
                if is_complete:
                    observer.on_completed()
                    self._finish()

                else:
                    if self._received_messages == limit_rate:
                        self._received_messages = 0
                        get_next_n.set()

            def _finish(self):
                done.set()
                self.is_done = True

            def on_error(self, exception: Exception):
                if self.is_done:
                    return
                observer.on_error(exception)
                self._finish()

            def on_complete(self):
                if self.is_done:
                    return
                observer.on_completed()
                self._finish()

        subscriber = RxSubscriber()

        async def _aio_sub():
            try:
                publisher.subscribe(subscriber)
                await done.wait()

                if not subscriber.is_done:
                    subscriber.subscription.cancel()

            except asyncio.CancelledError:
                if not subscriber.is_done:
                    subscriber.subscription.cancel()
            except Exception as exception:
                loop.call_soon(functools.partial(observer.on_error, exception))

        async def _trigger_next_request_n():
            try:
                while True:
                    nonlocal get_next_n
                    await get_next_n.wait()
                    subscriber.subscription.request(limit_rate)
                    get_next_n = asyncio.Event()
            except asyncio.CancelledError:
                pass

        get_next_n = asyncio.Event()
        get_next_task = asyncio.create_task(_trigger_next_request_n())
        task = asyncio.create_task(_aio_sub())

        def dispose():
            get_next_task.cancel()
            task.cancel()
            done.set()

        return Disposable(dispose)

    return rx.create(on_subscribe)
