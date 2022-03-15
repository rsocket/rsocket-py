import asyncio
import json
import logging
from uuid import uuid4

from reactivestreams.subscriber import DefaultSubscriber
from rsocket.extensions.helpers import composite, route, authenticate_simple
from rsocket.extensions.mimetypes import WellKnownMimeTypes
from rsocket.helpers import single_transport_provider
from rsocket.payload import Payload
from rsocket.rsocket_client import RSocketClient
from rsocket.transports.tcp import TransportTCP


class StreamSubscriber(DefaultSubscriber):

    def on_next(self, value, is_complete=False):
        logging.info('RS: {}'.format(value))
        self.subscription.request(1)


async def main():
    connection = await asyncio.open_connection('localhost', 7000)

    setup_payload = Payload(
        data=str(uuid4()).encode(),
        metadata=composite(route('shell-client'), authenticate_simple('user', 'pass')))

    async with RSocketClient(single_transport_provider(TransportTCP(*connection)),
                             setup_payload=setup_payload,
                             metadata_encoding=WellKnownMimeTypes.MESSAGE_RSOCKET_COMPOSITE_METADATA):
        await asyncio.sleep(5)


def serialize(message) -> bytes:
    return json.dumps(message).encode()


if __name__ == '__main__':
    logging.basicConfig(level=logging.DEBUG)
    asyncio.run(main())
