import asyncio
import json
import logging
import sys
from asyncio import Event
from typing import Optional

from reactivestreams.subscriber import DefaultSubscriber
from reactivestreams.subscription import Subscription
from rsocket.extensions.composite_metadata import CompositeMetadata
from rsocket.extensions.mimetypes import WellKnownMimeTypes
from rsocket.extensions.routing import RoutingMetadata
from rsocket.helpers import single_transport_provider
from rsocket.payload import Payload
from rsocket.rsocket_client import RSocketClient
from rsocket.transports.tcp import TransportTCP


async def main(server_port):
    completion_event = Event()

    class Subscriber(DefaultSubscriber):
        def __init__(self):
            super().__init__()
            self.values = []
            self.subscription: Optional[Subscription] = None

        def on_next(self, value, is_complete=False):
            self.values.append(value)
            self.subscription.request(1)

        def on_complete(self):
            completion_event.set()

        def on_error(self, exception: Exception):
            completion_event.set()

    connection = await asyncio.open_connection('localhost', server_port)

    async with RSocketClient(single_transport_provider(TransportTCP(*connection)),
                             metadata_encoding=WellKnownMimeTypes.MESSAGE_RSOCKET_COMPOSITE_METADATA.value.name,
                             data_encoding=WellKnownMimeTypes.APPLICATION_JSON.value.name) as client:
        metadata = CompositeMetadata()
        metadata.append(RoutingMetadata(['investigation.getInvestigationByContext']))

        body = json.dumps({'active': True}).encode()

        request = Payload(body, metadata.serialize())

        subscriber = Subscriber()
        client.request_stream(request).subscribe(subscriber)
        await completion_event.wait()

        await asyncio.sleep(4)  # Used to show keepalive is working

        assert len(subscriber.values) == 2


if __name__ == '__main__':
    port = sys.argv[1] if len(sys.argv) > 1 else 6565
    logging.basicConfig(level=logging.DEBUG)
    asyncio.run(main(port))
