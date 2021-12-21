from rsocket.extensions.composite_metadata import CompositeMetadataItem
from rsocket.extensions.mimetypes import WellKnownMimeTypes


class RoutingMetadata(CompositeMetadataItem):

    def __init__(self, route_path: bytes):
        super().__init__(WellKnownMimeTypes.MESSAGE_RSOCKET_ROUTING.value[0], route_path)
