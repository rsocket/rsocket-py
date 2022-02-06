import asyncio
import json
import logging
from asyncio import Event
from typing import Optional

from reactivestreams.subscriber import DefaultSubscriber
from reactivestreams.subscription import Subscription
from rsocket.extensions.composite_metadata import CompositeMetadata
from rsocket.extensions.mimetypes import WellKnownMimeTypes
from rsocket.extensions.routing import RoutingMetadata
from rsocket.payload import Payload
from rsocket.rsocket_client import RSocketClient
from rsocket.transports.tcp import TransportTCP


async def main():
    completion_event = Event()

    class Subscriber(DefaultSubscriber):
        def __init__(self):
            self.values = []
            self._subscription: Optional[Subscription] = None

        def on_subscribe(self, subscription: Subscription):
            self._subscription = subscription

        def on_next(self, value, is_complete=False):
            self.values.append(value)
            self._subscription.request(1)

        def on_complete(self):
            completion_event.set()

        def on_error(self, exception: Exception):
            completion_event.set()

    connection = await asyncio.open_connection('localhost', 6565)
    async with RSocketClient(TransportTCP(*connection),
                             metadata_encoding=WellKnownMimeTypes.MESSAGE_RSOCKET_COMPOSITE_METADATA.value.name,
                             data_encoding=WellKnownMimeTypes.APPLICATION_JSON.value.name) as client:
        metadata = CompositeMetadata()
        metadata.append(RoutingMetadata(['investigation.getInvestigationByContext']))

        body = bytes(bytearray(map(ord, json.dumps({'active': True}))))

        request = Payload(body, metadata.serialize())

        subscriber = Subscriber()
        client.request_stream(request).subscribe(subscriber)
        await completion_event.wait()

        await asyncio.sleep(4)  # Used to show keepalive is working

        assert len(subscriber.values) == 2


if __name__ == '__main__':
    logging.basicConfig(level=logging.DEBUG)
    asyncio.run(main())
