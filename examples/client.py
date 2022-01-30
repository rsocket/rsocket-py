import asyncio
import logging
from asyncio import Event

from reactivestreams.subscriber import Subscriber
from rsocket.payload import Payload
from rsocket.rsocket_client import RSocketClient
from rsocket.transports.tcp import TransportTCP


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
        self.subscription = subscription


async def main():
    connection = await asyncio.open_connection('localhost', 6565)

    async with RSocketClient(TransportTCP(*connection)) as client:
        payload = Payload(b'The quick brown fox', b'meta')

        result = await client.request_response(payload)
        logging.info('RR: {}'.format(result))

        completion_event = Event()
        client.request_stream(payload).subscribe(StreamSubscriber(completion_event))

        await completion_event.wait()


if __name__ == '__main__':
    logging.basicConfig(level=logging.DEBUG)
    asyncio.run(main())
