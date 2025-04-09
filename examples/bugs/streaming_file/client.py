import asyncio
import logging
from _codecs import utf_8_decode
from typing import List

from reactivex import operators

from rsocket.extensions.helpers import composite, route
from rsocket.extensions.mimetypes import WellKnownMimeTypes
from rsocket.helpers import single_transport_provider
from rsocket.payload import Payload
from rsocket.reactivex.reactivex_client import ReactiveXClient
from rsocket.rsocket_client import RSocketClient
from rsocket.transports.tcp import TransportTCP


def got_data(payload):
    logging.info('From server: ' + payload.data.decode('utf-8'))
    return utf_8_decode(payload.data)


class Client:

    def __init__(self, rs: RSocketClient):
        self._rs = rs

    async def stream_file(self) -> List[str]:
        request = Payload(metadata=composite(route('audio')))
        return await ReactiveXClient(self._rs).request_stream(request).pipe(
            operators.map(got_data),
            operators.to_list()
        )


async def main():
    connection = await asyncio.open_connection('localhost', 8000)
    async with RSocketClient(single_transport_provider(TransportTCP(*connection)),
                             metadata_encoding=WellKnownMimeTypes.MESSAGE_RSOCKET_COMPOSITE_METADATA,
                             fragment_size_bytes=1_000_000) as client:
        c = Client(client)
        r = await c.stream_file()
        print(r)

if __name__ == '__main__':
    logging.basicConfig(level=logging.DEBUG)
    asyncio.run(main())