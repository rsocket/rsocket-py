import pytest

from rsocket.connection import Connection
from rsocket.extensions.composite_metadata import CompositeMetadata
from rsocket.extensions.mimetypes import WellKnownMimeTypes
from rsocket.frame import (SetupFrame, CancelFrame, ErrorFrame, Type, RequestResponseFrame)
from tests.rsocket.helpers import data_bits, build_frame, bits


@pytest.fixture()
def decoder():
    return Connection()


def test_setup():
    connection = Connection()
    data = b'\x00\x00\x40\x00\x00\x00\x00\x05\x00\x00\x01\x00\x00\x00\x00\x00'
    data += b'\x7b\x00\x00\x01\xc8\x18\x61\x70\x70\x6c\x69\x63\x61\x74\x69\x6f'
    data += b'\x6e\x2f\x6f\x63\x74\x65\x74\x2d\x73\x74\x72\x65\x61\x6d\x09\x74'
    data += b'\x65\x78\x74\x2f\x68\x74\x6d\x6c\x00\x00\x05\x04\x05\x06\x07\x08'
    data += b'\x01\x02\x03'

    frame = connection.receive_data(data)[0]
    assert isinstance(frame, SetupFrame)
    assert frame.serialize() == data

    assert frame.metadata_encoding == b'application/octet-stream'
    assert frame.data_encoding == b'text/html'
    assert frame.keep_alive_milliseconds == 123
    assert frame.max_lifetime_milliseconds == 456
    assert frame.data == b'\x01\x02\x03'
    assert frame.metadata == b'\x04\x05\x06\x07\x08'


def test_setup_readable():
    connection = Connection()

    data = build_frame(
        bits(24, 64, 'Frame size'),
        bits(1, 0, 'Padding'),
        bits(31, 0, 'Stream id'),
        bits(6, 1, 'Frame type'),
        # Flags
        bits(1, 0, 'Ignore'),
        bits(1, 1, 'Metadata'),
        bits(1, 0, 'Resume'),
        bits(1, 0, 'Lease'),
        bits(6, 0, 'Padding flags'),
        # Version
        bits(16, 1, 'Major version'),
        bits(16, 0, 'Minor version'),
        # Timeouts
        bits(1, 0, 'Padding'),
        bits(31, 123, 'Time Between KEEPALIVE Frames'),
        bits(1, 0, 'Padding'),
        bits(31, 456, 'Max Lifetime'),
        # Resume token - not present
        # Meta-data mime
        bits(8, 24, 'Metadata mime length'),
        data_bits(b'application/octet-stream'),
        # Data mime
        bits(8, 9, 'Data mime length'),
        data_bits(b'text/html'),
        # Metadata
        bits(24, 5, 'Metadata length'),
        data_bits(b'\x04\x05\x06\x07\x08'),
        # Payload
        data_bits(b'\x01\x02\x03'),
    )

    frame = connection.receive_data(data)[0]
    assert isinstance(frame, SetupFrame)
    assert frame.serialize() == data

    assert frame.metadata_encoding == b'application/octet-stream'
    assert frame.data_encoding == b'text/html'
    assert frame.keep_alive_milliseconds == 123
    assert frame.max_lifetime_milliseconds == 456
    assert frame.data == b'\x01\x02\x03'
    assert frame.metadata == b'\x04\x05\x06\x07\x08'


def test_request_with_composite_metadata():
    connection = Connection()

    data = build_frame(
        bits(24, 27, 'Frame size'),
        bits(1, 0, 'Padding'),
        bits(31, 0, 'Stream id'),
        bits(6, Type.REQUEST_RESPONSE.value, 'Frame type'),
        # Flags
        bits(1, 0, 'Ignore'),
        bits(1, 1, 'Metadata'),
        bits(1, 0, 'Follows'),
        bits(7, 0, 'Empty flags'),
        # Metadata
        bits(24, 15, 'Metadata length'),
        # Composite metadata
        bits(1, 1, 'Well known metadata type'),
        bits(7, WellKnownMimeTypes.MESSAGE_RSOCKET_ROUTING.value[1], 'Mime ID'),
        bits(24, 11, 'Metadata length'),
        data_bits(b'target.path'),

        # Payload
        data_bits(b'\x01\x02\x03'),
    )

    frame = connection.receive_data(data)[0]
    assert isinstance(frame, RequestResponseFrame)
    assert frame.serialize() == data

    assert frame.data == b'\x01\x02\x03'

    metadata = CompositeMetadata()
    metadata.parse(frame.metadata, 0)

    assert metadata.items[0].metadata_encoding == b'message/x.rsocket.routing.v0'
    assert metadata.items[0].metadata == b'target.path'

    assert metadata.serialize() == frame.metadata


def test_cancel():
    connection = Connection()
    data = b'\x00\x00\x06\x00\x00\x00\x7b\x24\x00'
    frame, = connection.receive_data(data)
    assert isinstance(frame, CancelFrame)
    assert frame.serialize() == data


def test_error():
    connection = Connection()
    data = b'\x00\x00\x13\x00\x00\x26\x6a\x2c\x00\x00\x00\x02\x04\x77\x65\x69'
    data += b'\x72\x64\x6e\x65\x73\x73'
    frame = connection.receive_data(data)[0]
    assert isinstance(frame, ErrorFrame)
    assert frame.serialize() == data


def test_multiple_frames():
    connection = Connection()
    data = b'\x00\x00\x06\x00\x00\x00\x7b\x24\x00'
    data += b'\x00\x00\x13\x00\x00\x26\x6a\x2c\x00\x00\x00\x02\x04\x77\x65\x69'
    data += b'\x72\x64\x6e\x65\x73\x73'
    data += b'\x00\x00\x06\x00\x00\x00\x7b\x24\x00'
    data += b'\x00\x00\x06\x00\x00\x00\x7b\x24\x00'
    data += b'\x00\x00\x06\x00\x00\x00\x7b\x24\x00'
    frames = connection.receive_data(data)
    assert len(frames) == 5
