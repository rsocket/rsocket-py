from reactivex.abc import ObserverBase

from reactivestreams.subscriber import Subscriber


class SubscriberAdapter(ObserverBase):
    def __init__(self, subscriber: Subscriber):
        self._subscriber = subscriber

    def on_next(self, value):
        self._subscriber.on_next(value)

    def on_error(self, error):
        self._subscriber.on_error(error)

    def on_completed(self):
        self._subscriber.on_complete()
