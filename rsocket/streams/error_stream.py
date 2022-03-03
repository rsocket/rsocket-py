from rsocket.helpers import DefaultPublisherSubscription


class ErrorStream(DefaultPublisherSubscription):

    def __init__(self, exception: Exception):
        self._exception = exception

    def request(self, n: int):
        self._subscriber.on_error(self._exception)
