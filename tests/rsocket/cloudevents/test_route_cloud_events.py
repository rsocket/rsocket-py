import json

from cloudevents.conversion import to_json, from_json
from cloudevents.pydantic import CloudEvent

from rsocket.cloudevents.serialize import cloud_event_deserialize, cloud_event_serialize
from rsocket.extensions.helpers import composite, route
from rsocket.extensions.mimetypes import WellKnownMimeTypes
from rsocket.payload import Payload
from rsocket.routing.request_router import RequestRouter
from rsocket.routing.routing_request_handler import RoutingRequestHandler


async def test_routed_cloudevents(lazy_pipe):
    router = RequestRouter(cloud_event_deserialize,
                           cloud_event_serialize)

    def handler_factory():
        return RoutingRequestHandler(router)

    @router.response('cloud_event')
    async def response_request(value: CloudEvent) -> CloudEvent:
        return CloudEvent.create(attributes={
            'type': 'io.spring.event.Foo',
            'source': 'https://spring.io/foos'
        }, data=json.dumps(json.loads(value.data)))

    async with lazy_pipe(
            client_arguments={'metadata_encoding': WellKnownMimeTypes.MESSAGE_RSOCKET_COMPOSITE_METADATA},
            server_arguments={'handler_factory': handler_factory}) as (server, client):
        event = CloudEvent.create(attributes={
            'type': 'io.spring.event.Foo',
            'source': 'https://spring.io/foos'
        }, data=json.dumps({'value': 'Dave'}))

        response = await client.request_response(Payload(data=to_json(event), metadata=composite(route('cloud_event'))))

        response_event = from_json(CloudEvent, response.data)
        response_data = json.loads(response_event.data)

        assert response_data['value'] == 'Dave'
