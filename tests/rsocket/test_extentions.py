from rsocket.extensions.authentication import AuthenticationBearer, AuthenticationSimple
from rsocket.extensions.routing import RoutingMetadata


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

