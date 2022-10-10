import asyncio
import json

import pytest

from reactivestreams.subscriber import DefaultSubscriber
from rsocket.awaitable.awaitable_rsocket import AwaitableRSocket
from rsocket.extensions.authentication import Authentication, AuthenticationSimple
from rsocket.extensions.composite_metadata import CompositeMetadata
from rsocket.extensions.helpers import route, composite, authenticate_simple
from rsocket.extensions.mimetypes import WellKnownMimeTypes
from rsocket.helpers import create_future
from rsocket.payload import Payload
from rsocket.routing.request_router import RequestRouter
from rsocket.routing.routing_request_handler import RoutingRequestHandler
from rsocket.rx_support.rx_rsocket import RxRSocket
from rsocket.streams.stream_from_generator import StreamFromGenerator


async def test_routed_request_stream_properly_finished(lazy_pipe):
    router = RequestRouter()

    def handler_factory(socket):
        return RoutingRequestHandler(socket, router)

    def feed():
        for x in range(3):
            yield Payload('Feed Item: {}'.format(x).encode('utf-8')), x == 2

    @router.stream('test.path')
    async def response_stream(payload, composite_metadata):
        return StreamFromGenerator(feed)

    async with lazy_pipe(
            client_arguments={'metadata_encoding': WellKnownMimeTypes.MESSAGE_RSOCKET_COMPOSITE_METADATA},
            server_arguments={'handler_factory': handler_factory}) as (server, client):
        received_messages = await AwaitableRSocket(client).request_stream(
            Payload(metadata=composite(route('test.path'))))

        assert len(received_messages) == 3
        assert received_messages[0].data == b'Feed Item: 0'
        assert received_messages[1].data == b'Feed Item: 1'
        assert received_messages[2].data == b'Feed Item: 2'


async def test_routed_request_response_properly_finished(lazy_pipe):
    router = RequestRouter()

    def handler_factory(socket):
        return RoutingRequestHandler(socket, router)

    @router.response('test.path')
    async def response():
        return create_future(Payload(b'result'))

    async with lazy_pipe(
            client_arguments={'metadata_encoding': WellKnownMimeTypes.MESSAGE_RSOCKET_COMPOSITE_METADATA},
            server_arguments={'handler_factory': handler_factory}) as (server, client):
        result = await client.request_response(Payload(metadata=composite(route('test.path'))))

        assert result.data == b'result'


async def test_routed_request_response_with_payload_mapper(lazy_pipe):
    router = RequestRouter(lambda cls, _: json.loads(_.data.decode()))

    def handler_factory(socket):
        return RoutingRequestHandler(socket, router)

    @router.response('test.path')
    async def response(payload: dict):
        return create_future(Payload(('Response %s' % payload['key']).encode()))

    async with lazy_pipe(
            client_arguments={'metadata_encoding': WellKnownMimeTypes.MESSAGE_RSOCKET_COMPOSITE_METADATA},
            server_arguments={'handler_factory': handler_factory}) as (server, client):
        result = await client.request_response(Payload(data=json.dumps({'key': 'value'}).encode(),
                                                       metadata=composite(route('test.path'))))

        assert result.data == b'Response value'


async def test_routed_request_response_properly_finished_accept_payload_only(lazy_pipe):
    router = RequestRouter()

    def handler_factory(socket):
        return RoutingRequestHandler(socket, router)

    @router.response('test.path')
    async def response(payload: Payload):
        return create_future(Payload(('Response %s' % payload.data.decode()).encode()))

    async with lazy_pipe(
            client_arguments={'metadata_encoding': WellKnownMimeTypes.MESSAGE_RSOCKET_COMPOSITE_METADATA},
            server_arguments={'handler_factory': handler_factory}) as (server, client):
        result = await client.request_response(Payload(data=b'request', metadata=composite(route('test.path'))))

        assert result.data == b'Response request'


async def test_routed_request_response_properly_finished_accept_metadata_only(lazy_pipe):
    router = RequestRouter()

    def handler_factory(socket):
        return RoutingRequestHandler(socket, router)

    @router.response('test.path')
    async def response(composite_metadata: CompositeMetadata):
        return create_future(Payload(metadata=composite_metadata.items[0].tags[0]))

    async with lazy_pipe(
            client_arguments={'metadata_encoding': WellKnownMimeTypes.MESSAGE_RSOCKET_COMPOSITE_METADATA},
            server_arguments={'handler_factory': handler_factory}) as (server, client):
        result = await client.request_response(Payload(metadata=composite(route('test.path'))))

        assert result.metadata == b'test.path'


async def test_routed_request_response_properly_finished_accept_payload_and_metadata(lazy_pipe):
    router = RequestRouter()

    def handler_factory(socket):
        return RoutingRequestHandler(socket, router)

    @router.response('test.path')
    async def response(payload: Payload, composite_metadata: CompositeMetadata):
        return create_future(Payload(('Response %s' % payload.data.decode()).encode(),
                                     composite_metadata.items[0].tags[0]))

    async with lazy_pipe(
            client_arguments={'metadata_encoding': WellKnownMimeTypes.MESSAGE_RSOCKET_COMPOSITE_METADATA},
            server_arguments={'handler_factory': handler_factory}) as (server, client):
        result = await client.request_response(Payload(data=b'request', metadata=composite(route('test.path'))))

        assert result.data == b'Response request'
        assert result.metadata == b'test.path'


async def test_routed_fire_and_forget(lazy_pipe):
    router = RequestRouter()
    received_data = None
    received = asyncio.Event()

    def handler_factory(socket):
        return RoutingRequestHandler(socket, router)

    @router.fire_and_forget('test.path')
    async def fire_and_forget(payload):
        nonlocal received_data
        received_data = payload.data
        received.set()

    async with lazy_pipe(
            client_arguments={'metadata_encoding': WellKnownMimeTypes.MESSAGE_RSOCKET_COMPOSITE_METADATA},
            server_arguments={'handler_factory': handler_factory}) as (server, client):
        client.fire_and_forget(Payload(b'request data', composite(route('test.path'))))

        await received.wait()
        assert received_data == b'request data'


async def test_routed_request_channel_properly_finished(lazy_pipe):
    router = RequestRouter()

    def handler_factory(socket):
        return RoutingRequestHandler(socket, router)

    def feed():
        for x in range(3):
            yield Payload('Feed Item: {}'.format(x).encode('utf-8')), x == 2

    @router.channel('test.path')
    async def response_stream():
        return StreamFromGenerator(feed), DefaultSubscriber()

    async with lazy_pipe(
            client_arguments={'metadata_encoding': WellKnownMimeTypes.MESSAGE_RSOCKET_COMPOSITE_METADATA},
            server_arguments={'handler_factory': handler_factory}) as (server, client):
        received_messages = await AwaitableRSocket(client).request_channel(
            Payload(metadata=composite(route('test.path'))))

        assert len(received_messages) == 3
        assert received_messages[0].data == b'Feed Item: 0'
        assert received_messages[1].data == b'Feed Item: 1'
        assert received_messages[2].data == b'Feed Item: 2'


async def test_routed_push_metadata(lazy_pipe):
    router = RequestRouter()
    received_metadata = None
    received = asyncio.Event()

    def handler_factory(socket):
        return RoutingRequestHandler(socket, router)

    @router.metadata_push('test.path')
    async def metadata_push(payload):
        nonlocal received_metadata
        received_metadata = payload.metadata
        received.set()

    async with lazy_pipe(
            client_arguments={'metadata_encoding': WellKnownMimeTypes.MESSAGE_RSOCKET_COMPOSITE_METADATA},
            server_arguments={'handler_factory': handler_factory}) as (server, client):
        metadata = composite(route('test.path'))
        await client.metadata_push(metadata)

        await received.wait()
        assert received_metadata == metadata


async def test_invalid_request_response(lazy_pipe):
    router = RequestRouter()

    def handler_factory(socket):
        return RoutingRequestHandler(socket, router)

    @router.response('test.path')
    async def request_response():
        raise Exception('error from server')

    async with lazy_pipe(
            client_arguments={'metadata_encoding': WellKnownMimeTypes.MESSAGE_RSOCKET_COMPOSITE_METADATA},
            server_arguments={'handler_factory': handler_factory}) as (server, client):
        with pytest.raises(Exception) as exc_info:
            await client.request_response(Payload(metadata=composite(route('test.path'))))

        assert str(exc_info.value) == 'error from server'


async def test_invalid_request_stream(lazy_pipe):
    router = RequestRouter()

    def handler_factory(socket):
        return RoutingRequestHandler(socket, router)

    @router.stream('test.path')
    async def request_stream():
        raise Exception('error from server')

    async with lazy_pipe(
            client_arguments={'metadata_encoding': WellKnownMimeTypes.MESSAGE_RSOCKET_COMPOSITE_METADATA},
            server_arguments={'handler_factory': handler_factory}) as (server, client):
        with pytest.raises(Exception) as exc_info:
            await RxRSocket(client).request_stream(Payload(metadata=composite(route('test.path'))))

        assert str(exc_info.value) == 'error from server'


async def test_invalid_request_channel(lazy_pipe):
    router = RequestRouter()

    def handler_factory(socket):
        return RoutingRequestHandler(socket, router)

    @router.channel('test.path')
    async def request_channel():
        raise Exception('error from server')

    async with lazy_pipe(
            client_arguments={'metadata_encoding': WellKnownMimeTypes.MESSAGE_RSOCKET_COMPOSITE_METADATA},
            server_arguments={'handler_factory': handler_factory}) as (server, client):
        with pytest.raises(Exception) as exc_info:
            await RxRSocket(client).request_channel(Payload(metadata=composite(route('test.path'))))

        assert str(exc_info.value) == 'error from server'


async def test_no_route_in_request(lazy_pipe):
    router = RequestRouter()

    def handler_factory(socket):
        return RoutingRequestHandler(socket, router)

    async with lazy_pipe(
            client_arguments={'metadata_encoding': WellKnownMimeTypes.MESSAGE_RSOCKET_COMPOSITE_METADATA},
            server_arguments={'handler_factory': handler_factory}) as (server, client):
        with pytest.raises(Exception) as exc_info:
            await RxRSocket(client).request_channel(Payload(metadata=composite(authenticate_simple('user', 'pass'))))

        assert str(exc_info.value) == 'No route found in request'


async def test_invalid_authentication_in_routing_handler(lazy_pipe):
    router = RequestRouter()

    async def authenticate(path: str, authentication: Authentication):
        if not isinstance(authentication, AuthenticationSimple) or authentication.password != b'pass':
            raise Exception('Invalid credentials')

    @router.channel('test.path')
    async def request_channel():
        raise Exception('error from server')

    def handler_factory(socket):
        return RoutingRequestHandler(socket, router, authentication_verifier=authenticate)

    async with lazy_pipe(
            client_arguments={'metadata_encoding': WellKnownMimeTypes.MESSAGE_RSOCKET_COMPOSITE_METADATA},
            server_arguments={'handler_factory': handler_factory}) as (server, client):
        with pytest.raises(Exception) as exc_info:
            await RxRSocket(client).request_channel(
                Payload(metadata=composite(route('test.path'),
                                           authenticate_simple('user', 'wrong_password')))
            )

        assert str(exc_info.value) == 'Invalid credentials'


async def test_valid_authentication_in_routing_handler(lazy_pipe):
    router = RequestRouter()

    async def authenticate(path: str, authentication: Authentication):
        if not isinstance(authentication, AuthenticationSimple) or authentication.password != b'pass':
            raise Exception('Invalid credentials')

    @router.response('test.path')
    async def response():
        return create_future(Payload(b'result'))

    def handler_factory(socket):
        return RoutingRequestHandler(socket, router, authentication_verifier=authenticate)

    async with lazy_pipe(
            client_arguments={'metadata_encoding': WellKnownMimeTypes.MESSAGE_RSOCKET_COMPOSITE_METADATA},
            server_arguments={'handler_factory': handler_factory}) as (server, client):
        result = await RxRSocket(client).request_response(Payload(metadata=composite(route('test.path'),
                                                                                     authenticate_simple('user',
                                                                                                         'pass'))))

        assert result.data == b'result'
