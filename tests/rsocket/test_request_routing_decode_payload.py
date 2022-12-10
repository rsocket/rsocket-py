import json
from dataclasses import dataclass

from rsocket.extensions.helpers import route, composite
from rsocket.extensions.mimetypes import WellKnownMimeTypes
from rsocket.frame_helpers import ensure_bytes
from rsocket.helpers import utf8_decode, create_response
from rsocket.payload import Payload
from rsocket.routing.request_router import RequestRouter
from rsocket.routing.routing_request_handler import RoutingRequestHandler


async def test_request_response_type_hinted_payload(lazy_pipe):
    @dataclass
    class Message:
        user: str
        message: str

    router = RequestRouter(lambda cls, payload: cls(**json.loads(utf8_decode(payload.data))))

    def handler_factory():
        return RoutingRequestHandler(router)

    @router.response('test.path')
    async def response(message: Message):
        return create_response(ensure_bytes(message.message))

    async with lazy_pipe(
            client_arguments={'metadata_encoding': WellKnownMimeTypes.MESSAGE_RSOCKET_COMPOSITE_METADATA},
            server_arguments={'handler_factory': handler_factory}) as (server, client):
        result = await client.request_response(Payload(
            data=ensure_bytes(json.dumps(Message('George', 'hello').__dict__)),
            metadata=composite(route('test.path'))))

        assert result.data == b'hello'
