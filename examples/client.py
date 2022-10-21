import asyncio
import logging
import sys

from examples.shared_tests import simple_client_server_test
from reactivestreams.subscriber import DefaultSubscriber
from rsocket.helpers import single_transport_provider
from rsocket.rsocket_client import RSocketClient
from rsocket.transports.tcp import TransportTCP


class StreamSubscriber(DefaultSubscriber):

    def on_next(self, value, is_complete=False):
        logging.info('RS: {}'.format(value))
        self.subscription.request(1)


async def main(server_port):
    logging.info('Connecting to server at localhost:%s', server_port)

    connection = await asyncio.open_connection('localhost', server_port)

    async with RSocketClient(single_transport_provider(TransportTCP(*connection))) as client:
        await simple_client_server_test(client)


if __name__ == '__main__':
    port = sys.argv[1] if len(sys.argv) > 1 else 6565
    logging.basicConfig(level=logging.DEBUG)
    asyncio.run(main(port))
