import logging
from datetime import timedelta
from typing import AsyncGenerator, Tuple, Optional

from reactivestreams.subscriber import Subscriber
from reactivestreams.subscription import Subscription
from rsocket.fragment import Fragment
from rsocket.payload import Payload
from rsocket.streams.stream_from_async_generator import StreamFromAsyncGenerator
from rsocket.streams.stream_from_generator import StreamFromGenerator


def response_stream_2(response_count: int = 3,
                      delay_between_messages=timedelta(0),
                      fragment_size: Optional[int] = None):
    def generator():
        current_response = 0
        for i in range(response_count):
            is_complete = (current_response + 1) == response_count

            if delay_between_messages.total_seconds() > 0:
                message = 'Slow Item'
            else:
                message = 'Item'

            message = '%s: %s' % (message, current_response)
            yield Payload(message.encode('utf-8'), b'metadata'), is_complete

            if is_complete:
                break

            current_response += 1

    return StreamFromGenerator(generator, delay_between_messages, fragment_size)


def response_stream_1(response_count: int = 3,
                      local_subscriber: Optional[Subscriber] = None,
                      response_size: int = 10):
    async def generator() -> AsyncGenerator[Tuple[Fragment, bool], None]:
        current_response = 0
        message = b'a' * response_size

        for i in range(response_count):
            is_complete = (current_response + 1) == response_count

            yield Fragment(message), is_complete

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
