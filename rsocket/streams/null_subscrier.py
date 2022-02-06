from reactivestreams.subscriber import Subscriber
from reactivestreams.subscription import Subscription


class NullSubscriber(Subscriber):
    def on_next(self, value, is_complete=False):
        pass

    def on_error(self, exception: Exception):
        pass

    def on_complete(self):
        pass

    def on_subscribe(self, subscription: Subscription):
        pass
