from rx.core import Observer

from reactivestreams.subscriber import Subscriber
from reactivestreams.subscription import Subscription


class RxSubscriber(Subscriber):

    def __init__(self, observer: Observer):
        self._observer = observer

    def on_subscribe(self, subscription: Subscription):
        self._subscription = subscription

    def on_next(self, value, is_complete=False):
        self._observer.on_next(value)
        if is_complete:
            self.on_complete()

    def on_error(self, exception: Exception):
        self._observer.on_error(exception)

    def on_complete(self):
        self._observer.on_completed()
