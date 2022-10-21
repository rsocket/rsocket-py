from rsocket.extensions.composite_metadata_item import CompositeMetadataItem
from rsocket.extensions.helpers import metadata_item
from rsocket.extensions.mimetypes import WellKnownMimeTypes


def test_metadata_item():
    result = metadata_item(b'123', WellKnownMimeTypes.TEXT_PLAIN)

    assert result == CompositeMetadataItem(WellKnownMimeTypes.TEXT_PLAIN, b'123')
