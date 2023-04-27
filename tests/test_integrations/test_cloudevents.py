import json

from cloudevents.conversion import to_json, from_json
from cloudevents.pydantic import CloudEvent

from rsocket.extensions.helpers import route, composite
from rsocket.extensions.mimetypes import WellKnownMimeTypes
from rsocket.helpers import create_future
from rsocket.payload import Payload
from rsocket.routing.request_router import RequestRouter
from rsocket.routing.routing_request_handler import RoutingRequestHandler


async def test_routed_request_cloud_event(lazy_pipe):
    router = RequestRouter()

    def handler_factory():
        return RoutingRequestHandler(router)

    @router.response('event')
    async def single_request_response(payload):
        received_event = from_json(CloudEvent, payload.data)
        received_data = json.loads(received_event.data)

        event = CloudEvent.create(attributes={
            'type': 'io.spring.event.Foo',
            'source': 'https://spring.io/foos'
        }, data=json.dumps(received_data))

        return create_future(Payload(to_json(event)))

    async with lazy_pipe(
            client_arguments={'metadata_encoding': WellKnownMimeTypes.MESSAGE_RSOCKET_COMPOSITE_METADATA},
            server_arguments={'handler_factory': handler_factory}) as (server, client):
        event = CloudEvent.create(attributes={
            'type': 'io.spring.event.Foo',
            'source': 'https://spring.io/foos'
        }, data=json.dumps({'value': 'Dave'}))

        data = to_json(event)
        response = await client.request_response(Payload(data=data, metadata=composite(route('event'))))

        event = from_json(CloudEvent, response.data)
        response_data = json.loads(event.data)

        assert response_data['value'] == 'Dave'
