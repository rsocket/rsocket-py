from datetime import timedelta
from typing import Optional

from rsocket.payload import Payload
from rsocket.streams.stream_from_generator import StreamFromGenerator


def ResponseStream(response_count: int = 3,
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
