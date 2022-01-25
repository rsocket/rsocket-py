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
