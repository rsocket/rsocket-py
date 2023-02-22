from typing import cast

import pytest

from rsocket.exceptions import RSocketError
from rsocket.extensions.composite_metadata import CompositeMetadata
from rsocket.extensions.helpers import composite, data_mime_type, data_mime_types, route, authenticate_simple, \
    metadata_item
from rsocket.extensions.mimetypes import WellKnownMimeTypes
from rsocket.extensions.routing import RoutingMetadata
from rsocket.extensions.stream_data_mimetype import StreamDataMimetypes, StreamDataMimetype
from rsocket.logger import measure_runtime


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
    metadata_item_1 = cast(StreamDataMimetype, composite_metadata.items[0])
    assert metadata_item_1.data_encoding == b'application/json'

    assert composite_metadata.serialize() == data


def test_data_mime_types_composite_metadata():
    data = composite(data_mime_types(
        WellKnownMimeTypes.APPLICATION_JSON,
        WellKnownMimeTypes.TEXT_XML
    ))

    composite_metadata = CompositeMetadata()
    composite_metadata.parse(data)

    assert len(composite_metadata.items) == 1
    from typing import cast
    metadata_item_1 = cast(StreamDataMimetypes, composite_metadata.items[0])

    assert metadata_item_1.data_encodings[0] == b'application/json'
    assert metadata_item_1.data_encodings[1] == b'text/xml'

    assert composite_metadata.serialize() == data


def test_composite_metadata_find_by_mimetype():
    data = composite(
        data_mime_types(
            WellKnownMimeTypes.APPLICATION_JSON,
            WellKnownMimeTypes.TEXT_XML
        ),
        route('login'),
        authenticate_simple('abcd', '1234'),
        metadata_item(b'some_data_1', WellKnownMimeTypes.TEXT_PLAIN),
        metadata_item(b'some_data_2', WellKnownMimeTypes.TEXT_PLAIN),
        metadata_item(b'{"key":1}', WellKnownMimeTypes.APPLICATION_JSON),
    )

    composite_metadata = CompositeMetadata()
    composite_metadata.parse(data)

    plain_text = composite_metadata.find_by_mimetype(WellKnownMimeTypes.TEXT_PLAIN)

    assert len(plain_text) == 2
    assert plain_text[0].content == b'some_data_1'
    assert plain_text[1].content == b'some_data_2'

    authentication_items = composite_metadata.find_by_mimetype(WellKnownMimeTypes.MESSAGE_RSOCKET_AUTHENTICATION)
    assert authentication_items[0].authentication.password == b'1234'


def test_composite_metadata_parse_performance():
    data = composite(
        data_mime_types(
            WellKnownMimeTypes.APPLICATION_JSON,
            WellKnownMimeTypes.TEXT_XML
        ),
        route('login'),
        authenticate_simple('abcd', '1234'),
        metadata_item(b'some_data_1', WellKnownMimeTypes.TEXT_PLAIN),
        metadata_item(b'some_data_2', WellKnownMimeTypes.TEXT_PLAIN),
        metadata_item(b'{"key":1}', WellKnownMimeTypes.APPLICATION_JSON),
    )

    composite_metadata = CompositeMetadata()

    with measure_runtime() as result:
        for i in range(100):
            composite_metadata.items = []
            composite_metadata.parse(data)

    print(result.time.total_seconds() / 100 * 1000)
