import functools
from asyncio import Event, CancelledError, get_event_loop, create_task

import reactivex
from reactivex import Observable, Observer
from reactivex.disposable import Disposable

from reactivestreams.publisher import Publisher
from reactivestreams.subscriber import Subscriber
from reactivestreams.subscription import Subscription
from rsocket.frame import MAX_REQUEST_N
from rsocket.logger import logger


class RxSubscriber(Subscriber):
    def __init__(self, observer: Observer, limit_rate: int = MAX_REQUEST_N):
        self.limit_rate = limit_rate
        self.observer = observer
        self._received_messages = 0
        self.done = Event()
        self.get_next_n = Event()
        self.subscription = None

    def on_subscribe(self, subscription: Subscription):
        self.subscription = subscription

    def on_next(self, value, is_complete=False):
        self._received_messages += 1
        self.observer.on_next(value)

        if is_complete:
            self.observer.on_completed()
            self._finish()

        else:
            if self._received_messages == self.limit_rate:
                self._received_messages = 0
                self.get_next_n.set()

    def _finish(self):
        self.done.set()

    def on_error(self, exception: Exception):
        self.observer.on_error(exception)
        self._finish()

    def on_complete(self):
        self.observer.on_completed()
        self._finish()


async def _aio_sub(publisher: Publisher, subscriber: RxSubscriber, observer: Observer, loop):
    try:
        publisher.subscribe(subscriber)
        await subscriber.done.wait()

    except CancelledError:
        if not subscriber.done.is_set():
            subscriber.subscription.cancel()
    except Exception as exception:
        loop.call_soon(functools.partial(observer.on_error, exception))


async def _trigger_next_request_n(subscriber: RxSubscriber, limit_rate):
    try:
        while True:
            await subscriber.get_next_n.wait()
            subscriber.subscription.request(limit_rate)
            subscriber.get_next_n.clear()
    except CancelledError:
        logger().debug('Asyncio task canceled: trigger_next_request_n')


def from_rsocket_publisher(publisher: Publisher, limit_rate: int = MAX_REQUEST_N) -> Observable:
    loop = get_event_loop()

    # noinspection PyUnusedLocal
    def on_subscribe(observer: Observer, scheduler):
        subscriber = RxSubscriber(observer, limit_rate)

        get_next_task = create_task(
            _trigger_next_request_n(subscriber, limit_rate)
        )
        task = create_task(
            _aio_sub(publisher, subscriber, observer, loop)
        )

        def dispose():
            get_next_task.cancel()
            task.cancel()

        return Disposable(dispose)

    return reactivex.create(on_subscribe)


class RxSubscriberFromObserver(Subscriber):
    def __init__(self, observer: Observer, limit_rate: int):
        self.limit_rate = limit_rate
        self.observer = observer
        self._received_messages = 0
        self.subscription = None

    def on_subscribe(self, subscription: Subscription):
        self.subscription = subscription
        self.subscription.request(self.limit_rate)

    def on_next(self, value, is_complete=False):
        self._received_messages += 1
        self.observer.on_next(value)

        if is_complete:
            self.observer.on_completed()

        else:
            if self._received_messages == self.limit_rate:
                self._received_messages = 0
                self.subscription.request(self.limit_rate)

    def on_error(self, exception: Exception):
        self.observer.on_error(exception)

    def on_complete(self):
        self.observer.on_completed()
