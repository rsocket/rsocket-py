from rsocket.extensions.composite_metadata_item import CompositeMetadataItem
from rsocket.extensions.helpers import metadata_item
from rsocket.extensions.mimetypes import WellKnownMimeTypes
from rsocket.helpers import is_empty_payload, is_non_empty_payload
from rsocket.payload import Payload


def test_metadata_item():
    result = metadata_item(b'123', WellKnownMimeTypes.TEXT_PLAIN)

    assert result == CompositeMetadataItem(WellKnownMimeTypes.TEXT_PLAIN, b'123')


def test_is_empty_payload():
    assert is_empty_payload(Payload())
    assert not is_empty_payload(Payload(data=b'abc'))
    assert not is_empty_payload(Payload(metadata=b'abc'))
    assert not is_empty_payload(Payload(data=b'abc', metadata=b'abc'))


def test_is_non_empty_payload():
    assert not is_non_empty_payload(Payload())
    assert is_non_empty_payload(Payload(data=b'abc'))
    assert is_non_empty_payload(Payload(metadata=b'abc'))
    assert is_non_empty_payload(Payload(data=b'abc', metadata=b'abc'))
