import inspect
from typing import Callable, Any

from rsocket.extensions.composite_metadata import CompositeMetadata
from rsocket.payload import Payload
from rsocket.rsocket import RSocket

decorated_method = Callable[[RSocket, Payload, CompositeMetadata], Any]
channel_decorated_method = Callable[[RSocket, Payload, CompositeMetadata], Any]


class RequestRouter:
    __slots__ = (
        '_channel_routes',
        '_stream_routes',
        '_response_routes',
        '_fnf_routes',
        '_metadata_push',
        '_route_parameters'
    )

    def __init__(self):
        self._channel_routes = {}
        self._stream_routes = {}
        self._response_routes = {}
        self._fnf_routes = {}
        self._metadata_push = {}
        self._route_parameters = {}

    def _decorator_factory(self, container, route):
        def decorator(function: decorated_method):
            self._assert_not_route_already_registered(route)

            container[route] = function
            self._route_parameters[route] = inspect.getfullargspec(function)
            return function

        return decorator

    def response(self, route: str):
        return self._decorator_factory(self._response_routes, route)

    def stream(self, route: str):
        return self._decorator_factory(self._stream_routes, route)

    def channel(self, route: str):
        return self._decorator_factory(self._channel_routes, route)

    def fire_and_forget(self, route: str):
        return self._decorator_factory(self._fnf_routes, route)

    def metadata_push(self, route: str):
        return self._decorator_factory(self._metadata_push, route)

    def _assert_not_route_already_registered(self, route):
        if (route in self._fnf_routes
                or route in self._response_routes
                or route in self._stream_routes
                or route in self._channel_routes
                or route in self._metadata_push):
            raise KeyError('Duplicate route "%s" already registered', route)

    async def route(self,
                    route: str,
                    payload: Payload,
                    composite_metadata: CompositeMetadata):
        if route in self._fnf_routes:
            await self._fnf_routes[route](payload=payload, composite_metadata=composite_metadata)

        if route in self._response_routes:
            return await self._response_routes[route](payload=payload, composite_metadata=composite_metadata)

        if route in self._stream_routes:
            return await self._stream_routes[route](payload=payload, composite_metadata=composite_metadata)

        if route in self._channel_routes:
            return await self._channel_routes[route](payload=payload, composite_metadata=composite_metadata)

        if route in self._metadata_push:
            return await self._metadata_push[route](payload=payload, composite_metadata=composite_metadata)
