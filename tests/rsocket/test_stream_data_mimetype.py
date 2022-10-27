from rsocket.extensions.mimetypes import WellKnownMimeTypes
from rsocket.extensions.stream_data_mimetype import StreamDataMimetypes


def test_stream_data_mimetypes_equality():
    assert StreamDataMimetypes() == StreamDataMimetypes([])
    assert StreamDataMimetypes([WellKnownMimeTypes.APPLICATION_JSON]) == StreamDataMimetypes(
        [WellKnownMimeTypes.APPLICATION_JSON])
    assert StreamDataMimetypes([WellKnownMimeTypes.APPLICATION_JSON]) != StreamDataMimetypes(
        [WellKnownMimeTypes.TEXT_PLAIN])
