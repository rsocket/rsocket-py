import pytest

from rsocket.exceptions import RSocketError
from rsocket.extensions.composite_metadata import CompositeMetadata
from rsocket.extensions.mimetypes import WellKnownMimeTypes
from rsocket.extensions.routing import RoutingMetadata
from rsocket.extensions.helpers import composite, data_mime_type, data_mime_types


def test_tag_composite_metadata_too_long():
    routing = RoutingMetadata(tags=[('some data too long %s' % ''.join(['x'] * 256)).encode()])
    composite_metadata = CompositeMetadata()
    composite_metadata.items.append(routing)

    with pytest.raises(RSocketError):
        composite_metadata.serialize()


def test_data_mime_type_composite_metadata():
    data = composite(data_mime_type(WellKnownMimeTypes.APPLICATION_JSON))

    composite_metadata = CompositeMetadata()
    composite_metadata.parse(data)

    assert len(composite_metadata.items) == 1
    assert composite_metadata.items[0].data_encoding == b'application/json'

    assert composite_metadata.serialize() == data


def test_data_mime_types_composite_metadata():
    data = composite(data_mime_types(
        WellKnownMimeTypes.APPLICATION_JSON,
        WellKnownMimeTypes.TEXT_XML
    ))

    composite_metadata = CompositeMetadata()
    composite_metadata.parse(data)

    assert len(composite_metadata.items) == 1
    assert composite_metadata.items[0].data_encodings[0] == b'application/json'
    assert composite_metadata.items[0].data_encodings[1] == b'text/xml'

    assert composite_metadata.serialize() == data
