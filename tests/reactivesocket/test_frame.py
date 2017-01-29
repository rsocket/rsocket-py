import pytest

from reactivesocket.frame import (Type, SetupFrame, MetadataPushFrame,
                                  KeepAliveFrame)
from reactivesocket.connection import Connection


@pytest.fixture()
def decoder():
    return Connection()


def test_frames():
    connection = Connection()
    data = (b"\x00\x00\x00$\x00\x01 \x00\x00\x00\x00\x00\x00\x00\x00\x00\x00"
            b"\x00\x03\xe8\x00\x00'\x10\x05utf-8\x05utf-8" +
            b'\x00\x00\x00\x0c\x00\x03 \x00\x00\x00\x00\x00' +
            b'\x00\x00\x00\x12\x00\x04\x00\x00\x00\x00\x00\x02foobar' +
            b'\x00\x00\x00\x0c\x00\x03 \x00\x00\x00\x00\x00' +
            b'\x00\x00\x00\x0c\x00\x03 \x00\x00\x00\x00\x00')
    frames = connection.receive_data(data)
    assert len(frames) == 5


def test_setup():
    setup = SetupFrame()
    assert setup.frame_type == Type.SETUP
    setup.flags_metadata = True
    assert setup.flags_metadata


def test_metadata_push():
    push = MetadataPushFrame()
    assert push.frame_type == Type.METADATA_PUSH
    assert push.flags_metadata
