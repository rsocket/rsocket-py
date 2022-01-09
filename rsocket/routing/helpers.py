from rsocket.extensions.composite_metadata import CompositeMetadata
from rsocket.extensions.routing import RoutingMetadata


def route(path: str) -> bytes:
    metadata = CompositeMetadata()
    metadata.append(RoutingMetadata([path]))
    return metadata.serialize()
