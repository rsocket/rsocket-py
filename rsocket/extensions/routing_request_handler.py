from asyncio import Future
from typing import Callable, Union, Optional

from reactivestreams.publisher import Publisher
from rsocket import Payload
from rsocket.extensions.composite_metadata import CompositeMetadata
from rsocket.extensions.mimetypes import WellKnownMimeTypes
from rsocket.extensions.routing import RoutingMetadata
from rsocket.rsocket import BaseRequestHandler


class RoutingRequestHandler(BaseRequestHandler):
    __slots__ = (
        'router',
        'data_encoding',
        'metadata_encoding'
    )

    def __init__(self,
                 socket,
                 router: Callable[[int, str, Payload, CompositeMetadata], Union[Publisher, Future]]):
        super().__init__(socket)
        self.router = router

    # noinspection PyAttributeOutsideInit
    def on_setup(self, data_encoding: bytes, metadata_encoding: bytes):

        if metadata_encoding != WellKnownMimeTypes.MESSAGE_RSOCKET_COMPOSITE_METADATA.value[0]:
            self._send_error(Exception('Setup frame did not specify composite metadata. required for routing handler'))
        else:
            self.data_encoding = data_encoding
            self.metadata_encoding = metadata_encoding
            super().on_setup(data_encoding, metadata_encoding)

    def request_channel(self, payload: Payload, publisher: Publisher):
        return super().request_channel(payload, publisher)

    def request_fire_and_forget(self, payload: Payload):
        super().request_fire_and_forget(payload)

    def request_response(self, payload: Payload) -> Future:
        return self._parse_and_route(payload)

    def request_stream(self, payload: Payload) -> Publisher:
        return self._parse_and_route(payload)

    def _parse_and_route(self, payload: Payload) -> Union[Future, Publisher, None]:
        composite_metadata = CompositeMetadata()
        composite_metadata.parse(payload.metadata)
        route = self._require_route(composite_metadata)

        return self.router(self.socket, route, payload, composite_metadata)

    def _require_route(self, composite_metadata) -> Optional[str]:
        for item in composite_metadata.items:
            if isinstance(item, RoutingMetadata):
                return item.tags[0].decode()

        raise Exception('No route found in request')
