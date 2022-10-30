from asyncio import Future
from typing import Callable, Union, Optional, Coroutine, Tuple

from reactivestreams.publisher import Publisher
from reactivestreams.subscriber import Subscriber
from rsocket.extensions.authentication import Authentication
from rsocket.extensions.authentication_content import AuthenticationContent
from rsocket.extensions.composite_metadata import CompositeMetadata
from rsocket.extensions.helpers import require_route
from rsocket.extensions.mimetypes import WellKnownMimeTypes
from rsocket.frame import FrameType
from rsocket.helpers import create_error_future
from rsocket.local_typing import Awaitable
from rsocket.logger import logger
from rsocket.payload import Payload
from rsocket.request_handler import BaseRequestHandler
from rsocket.routing.request_router import RequestRouter
from rsocket.streams.error_stream import ErrorStream
from rsocket.streams.null_subscrier import NullSubscriber


class RoutingRequestHandler(BaseRequestHandler):
    __slots__ = (
        'router',
        'data_encoding',
        'metadata_encoding',
        'authentication_verifier',
    )

    def __init__(self,
                 router: RequestRouter,
                 authentication_verifier: Optional[
                     Callable[[str, Authentication], Coroutine[None, None, None]]] = None):
        super().__init__()
        self.router = router
        self.authentication_verifier = authentication_verifier
        self.data_encoding = None
        self.metadata_encoding = None

    async def on_setup(self,
                       data_encoding: bytes,
                       metadata_encoding: bytes,
                       payload: Payload):

        if metadata_encoding != WellKnownMimeTypes.MESSAGE_RSOCKET_COMPOSITE_METADATA.value.name:
            raise Exception('Setup frame did not specify composite metadata. required for routing handler')
        else:
            self.data_encoding = data_encoding
            self.metadata_encoding = metadata_encoding
            await super().on_setup(data_encoding, metadata_encoding, payload)

    async def request_channel(self, payload: Payload) -> Tuple[Optional[Publisher], Optional[Subscriber]]:
        try:
            return await self._parse_and_route(FrameType.REQUEST_CHANNEL, payload)
        except Exception as exception:
            return ErrorStream(exception), NullSubscriber()

    async def request_fire_and_forget(self, payload: Payload):
        try:
            await self._parse_and_route(FrameType.REQUEST_FNF, payload)
        except Exception:
            logger().error('Fire and forget error: %s', payload, exc_info=True)

    async def request_response(self, payload: Payload) -> Awaitable[Payload]:
        try:
            return await self._parse_and_route(FrameType.REQUEST_RESPONSE, payload)
        except Exception as exception:
            return create_error_future(exception)

    async def request_stream(self, payload: Payload) -> Publisher:
        try:
            return await self._parse_and_route(FrameType.REQUEST_STREAM, payload)
        except Exception as exception:
            return ErrorStream(exception)

    async def on_metadata_push(self, payload: Payload):
        try:
            await self._parse_and_route(FrameType.METADATA_PUSH, payload)
        except Exception:
            logger().error('Metadata push error: %s', payload, exc_info=True)

    async def _parse_and_route(
            self,
            frame_type: FrameType,
            payload: Payload
    ) -> Union[Future, Publisher, None, Tuple[Optional[Publisher], Optional[Subscriber]]]:
        composite_metadata = self._parse_composite_metadata(payload.metadata)
        route = require_route(composite_metadata)
        await self._verify_authentication(route, composite_metadata)
        return await self.router.route(frame_type, route, payload, composite_metadata)

    async def _verify_authentication(self, route: str, composite_metadata: CompositeMetadata):
        if self.authentication_verifier is not None:
            for item in composite_metadata.items:
                if isinstance(item, AuthenticationContent):
                    await self.authentication_verifier(route, item.authentication)
                    return

            raise Exception('Authentication required but not provided')
