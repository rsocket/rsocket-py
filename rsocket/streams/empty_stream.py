from rsocket.helpers import DefaultPublisherSubscription


class EmptyStream(DefaultPublisherSubscription):
    def request(self, n: int):
        self._subscriber.on_complete()
