import io
import sys
import tempfile

import pytest

from rsocket.cli.command import parse_uri, build_composite_metadata, create_request_payload, get_metadata_value, \
    create_setup_payload, normalize_data, normalize_limit_rate, RequestType, get_request_type, parse_headers
from rsocket.extensions.helpers import route, authenticate_simple, authenticate_bearer
from rsocket.frame import MAX_REQUEST_N
from tests.rsocket.helpers import create_data


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


def test_get_metadata_value():
    result = get_metadata_value([], None)

    assert result is None


def test_create_setup_payload():
    result = create_setup_payload(None, None)

    assert result is None


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


def test_normalize_limit_rate():
    result = normalize_limit_rate(None)

    assert result == MAX_REQUEST_N


@pytest.mark.parametrize('is_request, stream, fnf, metadata_push, channel, interaction_model, expected', (
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
