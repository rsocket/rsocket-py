import logging
from typing import AsyncGenerator, Tuple, Optional

from reactivestreams.subscriber import Subscriber
from reactivestreams.subscription import Subscription
from rsocket.payload import Payload
from rsocket.streams.stream_from_async_generator import StreamFromAsyncGenerator


def response_stream_1(response_count: int = 3, local_subscriber: Optional[Subscriber] = None):
    async def generator() -> AsyncGenerator[Tuple[Payload, bool], None]:
        current_response = 0

        for i in range(response_count):
            is_complete = (current_response + 1) == response_count

            message = 'Item on channel: %s' % current_response
            yield Payload(message.encode('utf-8')), is_complete

            if local_subscriber is not None:
                local_subscriber.subscription.request(2)

            if is_complete:
                break

            current_response += 1

    return StreamFromAsyncGenerator(generator)


class LoggingSubscriber(Subscriber):
    def on_subscribe(self, subscription: Subscription):
        self.subscription = subscription

    def on_next(self, value: Payload, is_complete=False):
        logging.info('From client on channel: ' + value.data.decode('utf-8'))

    def on_error(self, exception: Exception):
        logging.error('Error on channel ' + str(exception))

    def on_complete(self):
        logging.info('Completed on channel')
