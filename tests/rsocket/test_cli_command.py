import io
import sys
import tempfile
from asyncio import sleep

import pytest
from asyncclick.testing import CliRunner
from decoy import Decoy

from rsocket.awaitable.awaitable_rsocket import AwaitableRSocket
from rsocket.cli.command import parse_uri, build_composite_metadata, create_request_payload, get_metadata_value, \
    create_setup_payload, normalize_data, normalize_limit_rate, RequestType, get_request_type, parse_headers, \
    normalize_metadata_mime_type, execute_request, command
from rsocket.extensions.helpers import route, authenticate_simple, authenticate_bearer
from rsocket.extensions.mimetypes import WellKnownMimeTypes
from rsocket.frame import MAX_REQUEST_N
from rsocket.helpers import create_future, create_response
from rsocket.payload import Payload
from rsocket.request_handler import BaseRequestHandler
from tests.rsocket.helpers import create_data
from tests.tools.fixtures_quart import pipe_factory_quart_websocket
from tests.tools.fixtures_tcp import pipe_factory_tcp


def test_parse_uri():
    parsed = parse_uri('tcp://localhost:6565')

    assert parsed.schema == 'tcp'
    assert parsed.port == 6565
    assert parsed.host == 'localhost'


def test_parse_uri_wss():
    parsed = parse_uri('wss://localhost/path')

    assert parsed.schema == 'wss'
    assert parsed.port is None
    assert parsed.host == 'localhost'
    assert parsed.path == 'path'


@pytest.mark.parametrize('route_path, auth_simple, auth_bearer, expected', (
        (None, None, None, []),
        ('path1', None, None, [route('path1')]),
        ('path1', 'user:pass', None, [route('path1'), authenticate_simple('user', 'pass')]),
        ('path1', None, 'token', [route('path1'), authenticate_bearer('token')]),
        ('path1', 'user:pass', 'token', Exception),
))
def test_build_composite_metadata(route_path, auth_simple, auth_bearer, expected):
    if isinstance(expected, list):
        actual = build_composite_metadata(auth_simple, route_path, auth_bearer)

        assert actual == expected
    else:
        with pytest.raises(expected):
            build_composite_metadata(auth_simple, route_path, auth_bearer)


def test_create_request_payload():
    payload = create_request_payload(
        None, None, None
    )

    assert payload.data is None
    assert payload.metadata is None


@pytest.mark.parametrize('composite_items, metadata, expected', (
        ([], None, None),
        ([], 'metadata1', b'metadata1'),
        ([route('somewhere')], b'metadata1', b'\xfe\x00\x00\n\tsomewhere'),
))
def test_get_metadata_value(composite_items, metadata, expected):
    result = get_metadata_value(composite_items, metadata)

    assert result == expected


@pytest.mark.parametrize('data, metadata, expected', (
        (None, None, None),
        ('data', None, Payload(b'data')),
        ('data', 'metadata', Payload(b'data', b'metadata')),
        (None, 'metadata', Payload(None, b'metadata')),
))
def test_create_setup_payload(data, metadata, expected):
    result = create_setup_payload(data, metadata)

    assert result == expected


def test_normalize_data():
    data = normalize_data(None, None)

    assert data is None


def test_normalize_data_from_file():
    with tempfile.NamedTemporaryFile() as fd:
        fixture_data = create_data(b'1234567890', 20)
        fd.write(fixture_data)
        fd.flush()

        data = normalize_data(None, fd.name)

        assert data == fixture_data


def test_normalize_data_from_stdin():
    fixture_data = create_data(b'1234567890', 20)
    stdin = io.BytesIO(fixture_data)
    sys.stdin = stdin

    data = normalize_data('-', None)

    assert data == fixture_data


def test_normalize_data_from_stdin_takes_precedence_over_load_from_file():
    with tempfile.NamedTemporaryFile() as fd:
        fixture_data_file = create_data(b'1234567890', 20)
        fd.write(fixture_data_file)
        fd.flush()

        fixture_data_stdin = create_data(b'0987654321', 20)
        stdin = io.BytesIO(fixture_data_stdin)
        sys.stdin = stdin

        data = normalize_data('-', fd.name)

        assert data == fixture_data_stdin


@pytest.mark.parametrize('limit_rate, expected', (
        (MAX_REQUEST_N, MAX_REQUEST_N),
        (None, MAX_REQUEST_N),
        (3, 3),
        (0, MAX_REQUEST_N),
        (-5, MAX_REQUEST_N),
))
def test_normalize_limit_rate(limit_rate, expected):
    result = normalize_limit_rate(limit_rate)

    assert result == expected


@pytest.mark.parametrize('is_request, stream, fnf, metadata_push, channel, interaction_model, expected', (
        (None, None, None, None, None, None, Exception),
        (True, None, None, None, None, None, RequestType.response),
        (None, True, None, None, None, None, RequestType.stream),
        (None, None, True, None, None, None, RequestType.fnf),
        (None, None, None, True, None, None, RequestType.metadata_push),
        (None, None, None, None, True, None, RequestType.channel),
        (None, None, None, None, None, 'request_channel', RequestType.channel),
        (None, None, None, None, True, RequestType.response, Exception),
        (None, None, None, True, True, None, Exception),
))
def test_get_request_type(is_request, stream, fnf, metadata_push, channel, interaction_model, expected):
    if isinstance(expected, RequestType):
        actual = get_request_type(is_request, stream, fnf, metadata_push, channel, interaction_model)

        assert actual == expected
    else:
        with pytest.raises(expected):
            get_request_type(is_request, stream, fnf, metadata_push, channel, interaction_model)


@pytest.mark.parametrize('headers, expected', (
        (None, None),
        (['a=b'], {'a': 'b'}),
        ([], None),
))
def test_parse_headers(headers, expected):
    actual = parse_headers(headers)

    assert actual == expected


@pytest.mark.parametrize('composite_items, metadata_mime_type, expected', (
        ([], 'application/json', 'application/json'),
        ([route('path')], 'application/json', WellKnownMimeTypes.MESSAGE_RSOCKET_COMPOSITE_METADATA),
))
def test_normalize_metadata_mime_type(composite_items, metadata_mime_type, expected):
    actual = normalize_metadata_mime_type(composite_items, metadata_mime_type)

    assert actual == expected


async def test_execute_request_response(decoy: Decoy):
    client = decoy.mock(cls=AwaitableRSocket)

    decoy.when(await client.request_response(Payload())).then_return(Payload(b'abc'))

    result = await execute_request(client, RequestType.response, 3, Payload())

    assert result.data == b'abc'


async def test_execute_request_stream(decoy: Decoy):
    client = decoy.mock(cls=AwaitableRSocket)

    decoy.when(await client.request_stream(Payload(), limit_rate=3)).then_return([Payload(b'abc')])

    result = await execute_request(client, RequestType.stream, 3, Payload())

    assert result[0].data == b'abc'


async def test_execute_request_channel(decoy: Decoy):
    client = decoy.mock(cls=AwaitableRSocket)

    decoy.when(await client.request_channel(Payload(), limit_rate=3)).then_return([Payload(b'abc')])

    result = await execute_request(client, RequestType.channel, 3, Payload())

    assert result[0].data == b'abc'


async def test_execute_request_fnf(decoy: Decoy):
    client = decoy.mock(cls=AwaitableRSocket)

    decoy.when(client.fire_and_forget(Payload())).then_return(create_future(None))

    result = await execute_request(client, RequestType.fnf, 3, Payload())

    assert result is None


async def test_execute_command_tcp_request(unused_tcp_port):
    class Handler(BaseRequestHandler):
        async def request_response(self, request: Payload):
            await sleep(4)
            return create_response(b'response from test')

    async with pipe_factory_tcp(unused_tcp_port,
                                client_arguments={},
                                server_arguments={'handler_factory': Handler}):
        runner = CliRunner()
        uri = f'tcp://localhost:{unused_tcp_port}'
        result = await runner.invoke(command, ['--request', uri])

        assert result.stdout == 'response from test\n'
        assert result.exit_code == 0


async def test_execute_command_websocket_request(unused_tcp_port):
    class Handler(BaseRequestHandler):
        async def request_response(self, request: Payload):
            await sleep(4)
            return create_response(b'response from test')

    async with pipe_factory_quart_websocket(unused_tcp_port,
                                            client_arguments={},
                                            server_arguments={'handler_factory': Handler}):
        runner = CliRunner()
        uri = f'ws://localhost:{unused_tcp_port}'
        result = await runner.invoke(command, ['--request', uri])

        assert result.stdout == 'response from test\n'
        assert result.exit_code == 0
