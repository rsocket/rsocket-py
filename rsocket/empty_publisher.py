from reactivestreams.publisher import Publisher
from reactivestreams.subscriber import Subscriber
from reactivestreams.subscription import Subscription


class EmptyPublisher(Publisher, Subscription):
    def subscribe(self, subscriber: Subscriber):
        subscriber.on_subscribe(self)
        subscriber.on_complete()

    def request(self, n: int):
        pass

    def cancel(self):
        pass
