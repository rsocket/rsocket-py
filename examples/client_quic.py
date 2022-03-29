import asyncio
import logging
import sys
from pathlib import Path

from aioquic.quic.configuration import QuicConfiguration

from reactivestreams.subscriber import DefaultSubscriber
from rsocket.helpers import single_transport_provider
from rsocket.payload import Payload
from rsocket.rsocket_client import RSocketClient
from rsocket.transports.aioquic_transport import rsocket_connect


class StreamSubscriber(DefaultSubscriber):

    def on_next(self, value, is_complete=False):
        logging.info('RS: {}'.format(value))
        self.subscription.request(1)


async def main(server_port):
    logging.info('Connecting to server at localhost:%s', server_port)

    client_configuration = QuicConfiguration(
        is_client=True
    )
    ca_file_path = Path(__file__).parent / 'certificates' / 'pycacert.pem'
    client_configuration.load_verify_locations(cafile=str(ca_file_path))

    async with rsocket_connect('localhost', server_port,
                               configuration=client_configuration) as transport:
        async with RSocketClient(single_transport_provider(transport)) as client:
            payload = Payload(b'%Y-%m-%d %H:%M:%S')

            async def run_request_response():
                try:
                    while True:
                        result = await client.request_response(payload)
                        logging.info('Response: {}'.format(result.data))
                        await asyncio.sleep(1)
                except asyncio.CancelledError:
                    pass

            task = asyncio.create_task(run_request_response())

            await asyncio.sleep(5)
            task.cancel()
            await task


if __name__ == '__main__':
    port = sys.argv[1] if len(sys.argv) > 1 else 6565
    logging.basicConfig(level=logging.DEBUG)
    asyncio.run(main(port))
