import asyncio
import json
import logging
import sys

from cloudevents.conversion import to_json, from_json
from cloudevents.pydantic import CloudEvent

from rsocket.extensions.helpers import composite, route
from rsocket.extensions.mimetypes import WellKnownMimeTypes
from rsocket.helpers import single_transport_provider
from rsocket.payload import Payload
from rsocket.rsocket_client import RSocketClient
from rsocket.transports.tcp import TransportTCP


async def main(server_port: int):
    connection = await asyncio.open_connection('localhost', server_port)

    async with RSocketClient(single_transport_provider(TransportTCP(*connection)),
                             metadata_encoding=WellKnownMimeTypes.MESSAGE_RSOCKET_COMPOSITE_METADATA,
                             data_encoding=b'application/cloudevents+json') as client:
        event = CloudEvent.create(attributes={
            'type': 'io.spring.event.Foo',
            'source': 'https://spring.io/foos'
        }, data=json.dumps({'value': 'Dave'}))

        response = await client.request_response(Payload(data=to_json(event), metadata=composite(route('event'))))

        response_event = from_json(CloudEvent, response.data)
        response_data = json.loads(response_event.data)

        assert response_data['value'] == 'Dave'

        print(response_data['value'])


if __name__ == '__main__':
    port = sys.argv[1] if len(sys.argv) > 1 else 7000
    logging.basicConfig(level=logging.DEBUG)
    asyncio.run(main(port))
