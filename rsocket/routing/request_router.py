from asyncio import Future
from dataclasses import dataclass
from inspect import signature
from typing import Callable, Any, Dict

from rsocket.exceptions import RSocketUnknownRoute, RSocketEmptyRoute
from rsocket.extensions.composite_metadata import CompositeMetadata
from rsocket.frame import FrameType
from rsocket.frame_helpers import safe_len
from rsocket.helpers import create_future
from rsocket.payload import Payload
from rsocket.rsocket import RSocket

__all__ = ['RequestRouter']

decorated_method = Callable[[RSocket, Payload, CompositeMetadata], Any]


class RouteInfo:
    def __init__(self, method):
        self.method: Callable = method
        self.signature = signature(method)


def decorator_factory(container: dict, route: str):
    def decorator(function: decorated_method):
        if safe_len(route) == 0:
            raise RSocketEmptyRoute(function.__name__)

        if route in container:
            raise KeyError('Duplicate route "%s" already registered', route)

        container[route] = RouteInfo(function)
        return function

    return decorator


@dataclass
class Handlers:
    response: callable = None
    stream: callable = None
    channel: callable = None
    fire_and_forget: callable = None
    metadata_push: callable = None


class RequestRouter:
    """
    Used to define routes for RSocket endpoints.

    Pass this to :class:`RoutingRequestHandler <rsocket.routing.routing_request_handler.RoutingRequestHandler>`
    to instantiate a handler using these routes.
    """

    __slots__ = (
        '_channel_routes',
        '_stream_routes',
        '_response_routes',
        '_fnf_routes',
        '_metadata_push',
        '_route_map_by_frame_type',
        '_payload_deserializer',
        '_payload_serializer',
        '_unknown'
    )

    def __init__(self,
                 payload_deserializer=lambda cls, _: _,
                 payload_serializer=lambda cls, _: _):
        self._payload_serializer = payload_serializer
        self._payload_deserializer = payload_deserializer
        self._channel_routes: Dict[str, RouteInfo] = {}
        self._stream_routes: Dict[str, RouteInfo] = {}
        self._response_routes: Dict[str, RouteInfo] = {}
        self._fnf_routes: Dict[str, RouteInfo] = {}
        self._metadata_push: Dict[str, RouteInfo] = {}

        self._unknown = Handlers()

        self._route_map_by_frame_type: Dict[int, Dict[str, RouteInfo]] = {
            FrameType.REQUEST_CHANNEL: self._channel_routes,
            FrameType.REQUEST_FNF: self._fnf_routes,
            FrameType.REQUEST_STREAM: self._stream_routes,
            FrameType.REQUEST_RESPONSE: self._response_routes,
            FrameType.METADATA_PUSH: self._metadata_push,
        }

    def response(self, route: str):
        return decorator_factory(self._response_routes, route)

    def response_unknown(self):
        def wrapper(function):
            self._unknown.response = RouteInfo(function)
            return function

        return wrapper

    def stream(self, route: str):
        return decorator_factory(self._stream_routes, route)

    def stream_unknown(self):
        def wrapper(function):
            self._unknown.stream = RouteInfo(function)
            return function

        return wrapper

    def channel(self, route: str):
        return decorator_factory(self._channel_routes, route)

    def channel_unknown(self):
        def wrapper(function):
            self._unknown.channel = RouteInfo(function)
            return function

        return wrapper

    def fire_and_forget(self, route: str):
        return decorator_factory(self._fnf_routes, route)

    def fire_and_forget_unknown(self):
        def wrapper(function):
            self._unknown.fire_and_forget = RouteInfo(function)
            return function

        return wrapper

    def metadata_push(self, route: str):
        return decorator_factory(self._metadata_push, route)

    def metadata_push_unknown(self):
        def wrapper(function):
            self._unknown.metadata_push = RouteInfo(function)
            return function

        return wrapper

    async def route(self,
                    frame_type: FrameType,
                    route: str,
                    payload: Payload,
                    composite_metadata: CompositeMetadata):

        if route in self._route_map_by_frame_type[frame_type]:
            route_info = self._route_map_by_frame_type[frame_type][route]
        else:
            route_info = self._get_unknown_route(frame_type)

        if route_info is None:
            raise RSocketUnknownRoute(route)

        route_kwargs = self._collect_route_arguments(route_info,
                                                     payload,
                                                     composite_metadata)

        result = await route_info.method(**route_kwargs)

        if frame_type == FrameType.REQUEST_RESPONSE and not isinstance(result, Future):
            if not isinstance(result, Payload):
                result = self._payload_serializer(route_info.signature.return_annotation, result)

            return create_future(result)

        return result

    def _collect_route_arguments(self,
                                 route_info: RouteInfo,
                                 payload: Payload,
                                 composite_metadata: CompositeMetadata):
        route_signature = route_info.signature
        route_kwargs = {}

        for parameter in route_signature.parameters:
            parameter_type = route_signature.parameters[parameter]

            if 'composite_metadata' == parameter or parameter_type is CompositeMetadata:
                route_kwargs['composite_metadata'] = composite_metadata
            else:
                payload_data = payload

                if parameter_type.annotation not in (Payload, parameter_type.empty):
                    payload_data = self._payload_deserializer(parameter_type.annotation, payload)

                route_kwargs[parameter] = payload_data

        return route_kwargs

    def _get_unknown_route(self, frame_type: FrameType) -> Callable:
        if frame_type == FrameType.REQUEST_RESPONSE:
            return self._unknown.response
        elif frame_type == FrameType.REQUEST_STREAM:
            return self._unknown.stream
        elif frame_type == FrameType.REQUEST_CHANNEL:
            return self._unknown.channel
        elif frame_type == FrameType.REQUEST_FNF:
            return self._unknown.fire_and_forget
        elif frame_type == FrameType.METADATA_PUSH:
            return self._unknown.metadata_push
