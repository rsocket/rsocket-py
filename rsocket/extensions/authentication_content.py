from typing import Optional, Type

from rsocket.extensions.authentication import Authentication, AuthenticationSimple, AuthenticationBearer
from rsocket.extensions.authentication_types import WellKnownAuthenticationTypes
from rsocket.extensions.composite_metadata_item import CompositeMetadataItem
from rsocket.extensions.mimetypes import WellKnownMimeTypes
from rsocket.helpers import serialize_well_known_encoding, parse_well_known_encoding


class AuthenticationContent(CompositeMetadataItem):
    __slots__ = 'authentication'

    def __init__(self, authentication: Optional[Authentication] = None):
        super().__init__(WellKnownMimeTypes.MESSAGE_RSOCKET_AUTHENTICATION.value.name, None)
        self.authentication = authentication

    def serialize(self) -> bytes:
        serialized = serialize_well_known_encoding(self.authentication.type, WellKnownAuthenticationTypes.get_by_name)

        serialized += self.authentication.serialize()

        return serialized

    def parse(self, buffer: bytes):
        authentication_type, offset = parse_well_known_encoding(buffer, WellKnownAuthenticationTypes.require_by_id)
        self.authentication = authentication_item_factory(authentication_type)()
        self.authentication.parse(buffer[offset:])


metadata_item_factory_by_type = {
    WellKnownAuthenticationTypes.SIMPLE.value.name: AuthenticationSimple,
    WellKnownAuthenticationTypes.BEARER.value.name: AuthenticationBearer,
}


def authentication_item_factory(metadata_encoding: bytes) -> Type[Authentication]:
    return metadata_item_factory_by_type[metadata_encoding]
