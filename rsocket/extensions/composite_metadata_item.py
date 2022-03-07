from typing import Union, Optional

from rsocket.extensions.mimetypes import WellKnownMimeTypes

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
                 encoding: Union[bytes, WellKnownMimeTypes] = _default,
                 body: Optional[bytes] = _default):
        self.encoding = default_or_value(encoding)
        self.content = default_or_value(body)

    def parse(self, buffer: bytes):
        self.content = buffer

    def serialize(self) -> bytes:
        return self.content
