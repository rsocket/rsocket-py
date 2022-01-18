from typing import Union, List, Optional

from rsocket.extensions.mimetypes import WellKnownMimeTypes
from rsocket.extensions.tagging import TaggingMetadata


class RoutingMetadata(TaggingMetadata):

    def __init__(self, tags: Optional[List[Union[bytes, str]]] = None):
        super().__init__(WellKnownMimeTypes.MESSAGE_RSOCKET_ROUTING.value.name, tags)
