import asyncio
import logging
from asyncio import Event

from reactivestreams.subscriber import Subscriber
from rsocket.payload import Payload
from rsocket.rsocket_client import RSocketClient


class StreamSubscriber(Subscriber):

    def __init__(self, wait_for_complete: Event):
        self._wait_for_complete = wait_for_complete

    async def on_next(self, value, is_complete=False):
        logging.info('RS: {}'.format(value))
        await self.subscription.request(1)

    def on_complete(self):
        logging.info('RS: Complete')
        self._wait_for_complete.set()

    def on_error(self, exception):
        logging.info('RS: error: {}'.format(exception))
        self._wait_for_complete.set()

    def on_subscribe(self, subscription):
        # noinspection PyAttributeOutsideInit
        self.subscription = subscription


async def download(reader, writer):
    socket = RSocketClient(reader, writer)

    payload = Payload(b'The quick brown fox', b'meta')

    result = await socket.request_response(payload)
    logging.info('RR: {}'.format(result))

    completion_event = Event()
    socket.request_stream(payload).subscribe(StreamSubscriber(completion_event))

    await completion_event.wait()

    await socket.close()


if __name__ == '__main__':
    logging.basicConfig(level=logging.DEBUG)
    loop = asyncio.get_event_loop()
    try:
        connection = loop.run_until_complete(asyncio.open_connection(
            'localhost', 6565))
        loop.run_until_complete(download(*connection))
    finally:
        loop.close()
