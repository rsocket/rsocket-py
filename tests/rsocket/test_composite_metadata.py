import pytest

from rsocket.exceptions import RSocketError
from rsocket.extensions.composite_metadata import CompositeMetadata
from rsocket.extensions.routing import RoutingMetadata


def test_tag_composite_metadata_too_long():
    routing = RoutingMetadata(tags=[('some data too long %s' % ''.join(['x'] * 256)).encode()])
    composite_metadata = CompositeMetadata()
    composite_metadata.items.append(routing)

    with pytest.raises(RSocketError):
        composite_metadata.serialize()
