import itertools
import logging
from typing import AsyncGenerator, Tuple, Optional

from reactivestreams.subscriber import Subscriber
from reactivestreams.subscription import Subscription
from rsocket.payload import Payload
from rsocket.streams.stream_from_async_generator import StreamFromAsyncGenerator


def sample_async_response_stream(response_count: int = 3,
                                 local_subscriber: Optional[Subscriber] = None,
                                 is_infinite_stream: bool = False):
    async def generator() -> AsyncGenerator[Tuple[Payload, bool], None]:
        try:
            current_response = 0

            def range_counter():
                return range(response_count)

            if not is_infinite_stream:
                counter = range_counter
            else:
                counter = itertools.count

            for i in counter():
                if is_infinite_stream:
                    is_complete = False
                else:
                    is_complete = (current_response + 1) == response_count

                message = 'Item on channel: %s' % current_response
                yield Payload(message.encode('utf-8')), is_complete

                if local_subscriber is not None:
                    local_subscriber.subscription.request(2)

                if is_complete:
                    break

                current_response += 1
        finally:
            logging.info('Closing async stream generator')

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
