import io
import sys
import tempfile

from rsocket.cli.command import parse_uri, build_composite_metadata, create_request_payload, get_metadata_value, \
    create_setup_payload, normalize_data, normalize_limit_rate
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


def test_build_composite_metadata():
    composite = build_composite_metadata(
        None, None, None
    )

    assert len(composite) == 0


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
