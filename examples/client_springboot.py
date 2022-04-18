import asyncio
import json
import logging
from uuid import uuid4

from rsocket.extensions.helpers import composite, route, authenticate_simple
from rsocket.extensions.mimetypes import WellKnownMimeTypes
from rsocket.helpers import single_transport_provider
from rsocket.payload import Payload
from rsocket.rsocket_client import RSocketClient
from rsocket.transports.tcp import TransportTCP


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
