from rsocket.extensions.authentication_types import WellKnownAuthenticationTypes
from rsocket.extensions.composite_metadata import CompositeMetadata
from rsocket.extensions.mimetypes import WellKnownMimeTypes
from tests.rsocket.helpers import bits, data_bits, build_frame


async def test_authentication_frame_bearer():
    data = build_frame(
        bits(1, 1, 'Well known metadata type'),
        bits(7, WellKnownMimeTypes.MESSAGE_RSOCKET_AUTHENTICATION.value.id, 'Mime ID'),
        bits(24, 9, 'Metadata length'),
        bits(1, 1, 'Well known authentication type'),
        bits(7, WellKnownAuthenticationTypes.BEARER.value.id, 'Authentication ID'),
        data_bits(b'abcd1234')
    )

    composite_metadata = CompositeMetadata()
    composite_metadata.parse(data)

    auth = composite_metadata.items[0].authentication
    assert auth.token == b'abcd1234'

    serialized_data = composite_metadata.serialize()

    assert serialized_data == data


async def test_authentication_frame_simple():
    data = build_frame(
        bits(1, 1, 'Well known metadata type'),
        bits(7, WellKnownMimeTypes.MESSAGE_RSOCKET_AUTHENTICATION.value.id, 'Mime ID'),
        bits(24, 19, 'Metadata length'),
        bits(1, 1, 'Well known authentication type'),
        bits(7, WellKnownAuthenticationTypes.SIMPLE.value.id, 'Authentication ID'),
        bits(16, 8, 'Username length'),
        data_bits(b'username'),
        data_bits(b'password')
    )

    composite_metadata = CompositeMetadata()
    composite_metadata.parse(data)

    auth = composite_metadata.items[0].authentication
    assert auth.username == b'username'
    assert auth.password == b'password'

    serialized_data = composite_metadata.serialize()

    assert serialized_data == data
