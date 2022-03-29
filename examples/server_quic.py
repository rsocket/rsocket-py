import asyncio
import logging
import sys
from datetime import datetime
from pathlib import Path

from aioquic.quic.configuration import QuicConfiguration

from rsocket.helpers import create_future
from rsocket.payload import Payload
from rsocket.request_handler import BaseRequestHandler
from rsocket.transports.aioquic_transport import rsocket_serve


class Handler(BaseRequestHandler):
    async def request_response(self, payload: Payload) -> asyncio.Future:
        await asyncio.sleep(0.1)  # Simulate not immediate process
        date_time_format = payload.data.decode('utf-8')
        formatted_date_time = datetime.now().strftime(date_time_format)
        return create_future(Payload(formatted_date_time.encode('utf-8')))


def run_server(server_port):
    logging.info('Starting server at localhost:%s', server_port)

    configuration = QuicConfiguration(
        is_client=False
    )

    certificates_path = Path(__file__).parent / 'certificates'
    configuration.load_cert_chain(certificates_path / 'ssl_cert.pem', certificates_path / 'ssl_key.pem')

    return rsocket_serve(host='localhost',
                         port=server_port,
                         configuration=configuration,
                         handler_factory=Handler)


if __name__ == '__main__':
    port = sys.argv[1] if len(sys.argv) > 1 else 6565
    logging.basicConfig(level=logging.DEBUG)

    loop = asyncio.get_event_loop()
    loop.run_until_complete(run_server(port))
    try:
        loop.run_forever()
    except KeyboardInterrupt:
        pass
