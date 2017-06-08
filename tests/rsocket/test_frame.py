import pytest

from rsocket.connection import Connection
from rsocket.frame import (SetupFrame, CancelFrame, ErrorFrame)


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

    frame, = connection.receive_data(data)
    assert isinstance(frame, SetupFrame)
    assert frame.serialize() == data

    assert frame.metadata_encoding == b'application/octet-stream'
    assert frame.data_encoding == b'text/html'
    assert frame.keep_alive_milliseconds == 123
    assert frame.max_lifetime_milliseconds == 456
    assert frame.data == b'\x01\x02\x03'
    assert frame.metadata == b'\x04\x05\x06\x07\x08'


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
    frame, = connection.receive_data(data)
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

