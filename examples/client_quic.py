import asyncio
import logging
import sys
from pathlib import Path

from aioquic.quic.configuration import QuicConfiguration

from examples.shared_tests import simple_client_server_test
from rsocket.helpers import single_transport_provider
from rsocket.rsocket_client import RSocketClient
from rsocket.transports.aioquic_transport import rsocket_connect


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
            await simple_client_server_test(client)


if __name__ == '__main__':
    port = sys.argv[1] if len(sys.argv) > 1 else 6565
    logging.basicConfig(level=logging.DEBUG)
    asyncio.run(main(port))
