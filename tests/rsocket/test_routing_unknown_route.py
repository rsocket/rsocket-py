from rsocket.awaitable.awaitable_rsocket import AwaitableRSocket
from rsocket.extensions.helpers import route, composite
from rsocket.extensions.mimetypes import WellKnownMimeTypes
from rsocket.frame_helpers import ensure_bytes
from rsocket.helpers import create_future
from rsocket.payload import Payload
from rsocket.routing.request_router import RequestRouter
from rsocket.routing.routing_request_handler import RoutingRequestHandler
from rsocket.streams.stream_from_generator import StreamFromGenerator


async def test_routed_request_response_unknown_route(lazy_pipe):
    router = RequestRouter()

    def handler_factory():
        return RoutingRequestHandler(router)

    @router.response_unknown()
    async def response():
        return create_future(Payload(b'fallback'))

    async with lazy_pipe(
            client_arguments={'metadata_encoding': WellKnownMimeTypes.MESSAGE_RSOCKET_COMPOSITE_METADATA},
            server_arguments={'handler_factory': handler_factory}) as (server, client):
        result = await client.request_response(Payload(metadata=composite(route('test.path'))))

        assert result.data == b'fallback'


async def test_routed_request_stream_unknown_route(lazy_pipe):
    router = RequestRouter()

    def handler_factory():
        return RoutingRequestHandler(router)

    @router.stream_unknown()
    async def response_stream(payload, composite_metadata):
        return StreamFromGenerator(
            lambda: ((Payload(ensure_bytes(str(i))), index == 9) for i, index in enumerate(range(10))))

    async with lazy_pipe(
            client_arguments={'metadata_encoding': WellKnownMimeTypes.MESSAGE_RSOCKET_COMPOSITE_METADATA},
            server_arguments={'handler_factory': handler_factory}) as (server, client):
        received_messages = await AwaitableRSocket(client).request_stream(
            Payload(metadata=composite(route('test.path'))))

        assert len(received_messages) == 10
