from typing import Union

from rsocket.extensions.authentication import AuthenticationBearer, AuthenticationSimple
from rsocket.extensions.authentication_content import AuthenticationContent
from rsocket.extensions.composite_metadata import CompositeMetadata, CompositeMetadataItem
from rsocket.extensions.mimetypes import WellKnownMimeType, WellKnownMimeTypes
from rsocket.extensions.routing import RoutingMetadata
from rsocket.extensions.stream_data_mimetype import StreamDataMimetype, StreamDataMimetypes


def composite(*items) -> bytes:
    metadata = CompositeMetadata()
    metadata.extend(*items)
    return metadata.serialize()


def metadata_item(data: bytes,
                  encoding: Union[bytes, WellKnownMimeTypes, WellKnownMimeType]) -> CompositeMetadataItem:
    return CompositeMetadataItem(encoding, data)


def authenticate_simple(username: str, password: str) -> CompositeMetadataItem:
    return AuthenticationContent(AuthenticationSimple(username, password))


def authenticate_bearer(token: str) -> CompositeMetadataItem:
    return AuthenticationContent(AuthenticationBearer(token))


def route(*paths: str) -> CompositeMetadataItem:
    return RoutingMetadata(list(paths))


def data_mime_type(metadata_mime_type: Union[bytes, WellKnownMimeType]) -> StreamDataMimetype:
    return StreamDataMimetype(metadata_mime_type)


def data_mime_types(*metadata_mime_types: Union[bytes, WellKnownMimeType]) -> StreamDataMimetypes:
    return StreamDataMimetypes(list(metadata_mime_types))


def require_route(composite_metadata: CompositeMetadata) -> str:
    for item in composite_metadata.items:
        if isinstance(item, RoutingMetadata):
            return item.tags[0].decode()

    raise Exception('No route found in request')
