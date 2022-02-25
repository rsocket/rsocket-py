import pytest

from rsocket.payload import Payload


@pytest.mark.parametrize('payload, expected_str', (
        (Payload(), "<payload: None, None>"),
        (Payload(b'some data'), "<payload: b'some data', None>"),
        (Payload(metadata=b'some metadata'), "<payload: None, b'some metadata'>"),
        (Payload(b'some data', b'some metadata'), "<payload: b'some data', b'some metadata'>"),

))
def test_payload_to_str(payload, expected_str):
    assert str(payload) == expected_str


@pytest.mark.parametrize('payload, expected_str', (
        (Payload(), "Payload(None, None)"),
        (Payload(b'some data'), "Payload(b'some data', None)"),
        (Payload(metadata=b'some metadata'), "Payload(None, b'some metadata')"),
        (Payload(b'some data', b'some metadata'), "Payload(b'some data', b'some metadata')"),

))
def test_payload_repr(payload, expected_str):
    assert repr(payload) == expected_str


def test_payload_support_bytearray():
    payload = Payload(bytearray([1, 5, 10]), bytearray([4, 6, 7]))

    assert payload.data == b'\x01\x05\x0a'
    assert payload.metadata == b'\x04\x06\x07'
