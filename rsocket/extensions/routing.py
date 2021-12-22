from typing import Union

from rsocket.extensions.composite_metadata import CompositeMetadataItem
from rsocket.extensions.mimetypes import WellKnownMimeTypes


class RoutingMetadata(CompositeMetadataItem):

    def __init__(self, route_path: Union[bytes, str]):
        if isinstance(route_path, str):
            route_path = bytes(bytearray(map(ord, route_path)))

        super().__init__(WellKnownMimeTypes.MESSAGE_RSOCKET_ROUTING.value[0], route_path)
