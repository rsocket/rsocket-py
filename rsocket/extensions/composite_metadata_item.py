from typing import Union, Optional

from rsocket.extensions.mimetypes import WellKnownMimeTypes, WellKnownMimeType, ensure_well_known_encoding_enum_value

_default = object()


def default_or_value(value, default=None):
    if value is _default:
        return default
    return value


class CompositeMetadataItem:
    __slots__ = (
        'encoding',
        'content'
    )

    def __init__(self,
                 encoding: Union[bytes, WellKnownMimeTypes, WellKnownMimeType] = _default,
                 body: Optional[bytes] = _default):
        self.encoding = ensure_well_known_encoding_enum_value(default_or_value(encoding))
        self.content = default_or_value(body)

    def parse(self, buffer: bytes):
        self.content = buffer

    def serialize(self) -> bytes:
        return self.content

    def __eq__(self, other):
        if isinstance(other, self.__class__):
            return self.content == other.content and self.encoding == other.encoding

        return False
