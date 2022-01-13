from rsocket.extensions.authentication import AuthenticationBearer, AuthenticationSimple
from rsocket.extensions.authentication_content import AuthenticationContent
from rsocket.extensions.composite_metadata import CompositeMetadata, CompositeMetadataItem
from rsocket.extensions.routing import RoutingMetadata


def composite(*items) -> bytes:
    metadata = CompositeMetadata()
    metadata.extend(*items)
    return metadata.serialize()


def authenticate_simple(username: str, password: str) -> CompositeMetadataItem:
    return AuthenticationContent(AuthenticationSimple(username, password))


def authenticate_bearer(token: str) -> CompositeMetadataItem:
    return AuthenticationContent(AuthenticationBearer(token))


def route(path: str) -> CompositeMetadataItem:
    return RoutingMetadata([path])
