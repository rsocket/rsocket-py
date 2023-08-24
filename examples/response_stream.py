import itertools
import logging
from datetime import timedelta

from rsocket.payload import Payload
from rsocket.streams.stream_from_generator import StreamFromGenerator


def sample_sync_response_stream(response_count: int = 3,
                                delay_between_messages=timedelta(0),
                                is_infinite_stream: bool = False):
    def generator():
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

                if delay_between_messages.total_seconds() > 0:
                    message = 'Slow Item'
                else:
                    message = 'Item'

                message = '%s: %s' % (message, current_response)
                yield Payload(message.encode('utf-8'), b'metadata'), is_complete

                if is_complete:
                    break

                current_response += 1
        finally:
            logging.info('Closing sync stream generator')

    return StreamFromGenerator(generator, delay_between_messages)
