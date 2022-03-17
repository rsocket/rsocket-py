from inspect import signature, Parameter
from typing import Callable, Any

from rsocket.extensions.composite_metadata import CompositeMetadata
from rsocket.frame import FrameType
from rsocket.payload import Payload
from rsocket.rsocket import RSocket

decorated_method = Callable[[RSocket, Payload, CompositeMetadata], Any]


def decorator_factory(container: dict, route: str):
    def decorator(function: decorated_method):
        if route in container:
            raise KeyError('Duplicate route "%s" already registered', route)

        container[route] = function
        return function

    return decorator


class RequestRouter:
    __slots__ = (
        '_channel_routes',
        '_stream_routes',
        '_response_routes',
        '_fnf_routes',
        '_metadata_push',
        '_route_map_by_frame_type',
        '_payload_mapper'
    )

    def __init__(self, payload_mapper=lambda cls, _: _):
        self._payload_mapper = payload_mapper
        self._channel_routes = {}
        self._stream_routes = {}
        self._response_routes = {}
        self._fnf_routes = {}
        self._metadata_push = {}

        self._route_map_by_frame_type = {
            FrameType.REQUEST_CHANNEL: self._channel_routes,
            FrameType.REQUEST_FNF: self._fnf_routes,
            FrameType.REQUEST_STREAM: self._stream_routes,
            FrameType.REQUEST_RESPONSE: self._response_routes,
            FrameType.METADATA_PUSH: self._metadata_push,
        }

    def response(self, route: str):
        return decorator_factory(self._response_routes, route)

    def stream(self, route: str):
        return decorator_factory(self._stream_routes, route)

    def channel(self, route: str):
        return decorator_factory(self._channel_routes, route)

    def fire_and_forget(self, route: str):
        return decorator_factory(self._fnf_routes, route)

    def metadata_push(self, route: str):
        return decorator_factory(self._metadata_push, route)

    async def route(self,
                    frame_type: FrameType,
                    route: str,
                    payload: Payload,
                    composite_metadata: CompositeMetadata):

        if route in self._route_map_by_frame_type[frame_type]:
            route_processor = self._route_map_by_frame_type[frame_type][route]
            route_kwargs = await self._collect_route_arguments(route_processor,
                                                               payload,
                                                               composite_metadata)

            return await route_processor(**route_kwargs)

    async def _collect_route_arguments(self, route_processor, payload, composite_metadata):
        route_signature = signature(route_processor)
        route_kwargs = {}

        if 'payload' in route_signature.parameters:
            payload_expected_type = route_signature.parameters['payload']

            if payload_expected_type is not Payload and payload_expected_type is not Parameter:
                payload = self._payload_mapper(payload_expected_type, payload)

            route_kwargs['payload'] = payload

        if 'composite_metadata' in route_signature.parameters:
            route_kwargs['composite_metadata'] = composite_metadata

        return route_kwargs
