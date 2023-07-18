import pytest
import struct

from rsocket.exceptions import RSocketError
from rsocket.extensions.authentication import AuthenticationBearer, AuthenticationSimple
from rsocket.extensions.mimetypes import WellKnownMimeTypes
from rsocket.extensions.routing import RoutingMetadata
from rsocket.extensions.tagging import TaggingMetadata


def test_authentication_bearer():
    data = b'1234'

    authentication = AuthenticationBearer(b'1234')

    assert authentication.serialize() == data

    parsed = AuthenticationBearer()
    parsed.parse(data)

    assert parsed == authentication


def test_authentication_simple():
    data = b'\x00\x041234abcd'

    authentication = AuthenticationSimple(b'1234', b'abcd')

    assert authentication.serialize() == data

    parsed = AuthenticationSimple()
    parsed.parse(data)

    assert parsed == authentication


def test_routing():
    data = b'\nroute.path\x0bother.route'

    routing = RoutingMetadata([b'route.path', b'other.route'])

    assert routing.serialize() == data

    parsed = RoutingMetadata()
    parsed.parse(data)

    assert parsed == routing


def test_tagging_metadata_serialize_max_length():
    tag = 's' * 255
    meta = TaggingMetadata(WellKnownMimeTypes.MESSAGE_RSOCKET_ROUTING, [tag])

    serialized = meta.serialize()

    length = struct.pack('>B', len(tag))
    assert length + bytes(tag, 'utf-8') == serialized


def test_tagging_metadata_serialize_exception_length():
    tag = 's' * 256
    meta = TaggingMetadata(WellKnownMimeTypes.MESSAGE_RSOCKET_ROUTING, [tag])

    with pytest.raises(RSocketError) as e_info:
        meta.serialize()

    assert e_info.match(f'Tag length longer than 255 characters: "b\'{tag}\'"')


def test_tagging_metadata_parse():
    meta = TaggingMetadata(WellKnownMimeTypes.MESSAGE_RSOCKET_ROUTING)
    tag = 's' * 255
    length = struct.pack('>B', len(tag))

    meta.parse(length + bytes(tag, 'utf-8'))
    assert tag == meta.tags[0].decode()
