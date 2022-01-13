from asyncio import Future
from typing import Callable, Union, Optional, Coroutine

from reactivestreams.publisher import Publisher
from reactivestreams.subscriber import Subscriber
from reactivestreams.subscription import Subscription
from rsocket.extensions.authentication import Authentication
from rsocket.extensions.authentication_content import AuthenticationContent
from rsocket.extensions.composite_metadata import CompositeMetadata
from rsocket.extensions.mimetypes import WellKnownMimeTypes
from rsocket.extensions.routing import RoutingMetadata
from rsocket.helpers import always_allow_authenticator
from rsocket.payload import Payload
from rsocket.rsocket import BaseRequestHandler


class RoutingRequestHandler(BaseRequestHandler):
    __slots__ = (
        'router',
        'data_encoding',
        'metadata_encoding',
        'authentication_verifier'
    )

    def __init__(self,
                 socket,
                 router: Callable[
                     [int, str, Payload, CompositeMetadata, Optional[Publisher]], Union[Publisher, Future]],
                 authentication_verifier: Optional[
                     Callable[[Authentication], Coroutine[None, None, None]]] = always_allow_authenticator):
        super().__init__(socket)
        self.authentication_verifier = authentication_verifier
        self.router = router

    # noinspection PyAttributeOutsideInit
    async def on_setup(self,
                       data_encoding: bytes,
                       metadata_encoding: bytes):

        if metadata_encoding != WellKnownMimeTypes.MESSAGE_RSOCKET_COMPOSITE_METADATA.value.name:
            raise Exception('Setup frame did not specify composite metadata. required for routing handler')
        else:
            self.data_encoding = data_encoding
            self.metadata_encoding = metadata_encoding
            await super().on_setup(data_encoding, metadata_encoding)

    async def request_channel(self, payload: Payload) -> Union[Publisher, Subscription, Subscriber]:
        return await self._parse_and_route(payload)

    async def request_fire_and_forget(self, payload: Payload):
        await self._parse_and_route(payload)

    async def request_response(self, payload: Payload) -> Future:
        return await self._parse_and_route(payload)

    async def request_stream(self, payload: Payload) -> Publisher:
        return await self._parse_and_route(payload)

    async def _parse_and_route(self, payload: Payload) -> Union[Future, Publisher, None]:
        composite_metadata = CompositeMetadata()
        composite_metadata.parse(payload.metadata)
        await self._verify_authentication(composite_metadata)
        route = self._require_route(composite_metadata)

        return self.router(self.socket, route, payload, composite_metadata, None)

    def _require_route(self, composite_metadata: CompositeMetadata) -> Optional[str]:
        for item in composite_metadata.items:
            if isinstance(item, RoutingMetadata):
                return item.tags[0].decode()

        raise Exception('No route found in request')

    async def _verify_authentication(self, composite_metadata: CompositeMetadata):
        if self.authentication_verifier is not None:
            for item in composite_metadata.items:
                if isinstance(item, AuthenticationContent):
                    await self.authentication_verifier(item.authentication)
                    return

            raise Exception('Authentication required but not provided')
