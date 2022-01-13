import inspect
from typing import Callable, Any, Optional

from reactivestreams.publisher import Publisher
from rsocket.extensions.composite_metadata import CompositeMetadata
from rsocket.payload import Payload
from rsocket.rsocket import RSocket

decorated_method = Callable[[RSocket, Payload, CompositeMetadata], Any]
channel_decorated_method = Callable[[RSocket, Payload, CompositeMetadata], Any]


class RequestRouter:
    __slots__ = '_routes', '_route_parameters'

    def __init__(self):
        self._routes = {}
        self._route_parameters = {}

    def response(self, route: str):
        def decorator(function: decorated_method):
            return self._register_method(route, function)

        return decorator

    def stream(self, route: str):
        def decorator(function: decorated_method):
            return self._register_method(route, function)

        return decorator

    def channel(self, route: str):
        def decorator(function: channel_decorated_method):
            return self._register_method(route, function)

        return decorator

    def fire_and_forget(self, route: str):
        def decorator(function: decorated_method):
            return self._register_method(route, function)

        return decorator

    def _register_method(self, route: str, function: decorated_method):
        if route in self._routes:
            raise KeyError('Duplicate route "%s" already registered', route)

        self._routes[route] = function
        self._route_parameters[route] = inspect.getfullargspec(function)
        return function

    def __call__(self,
                 socket,
                 route: str,
                 payload: Payload,
                 composite_metadata: CompositeMetadata,
                 publisher: Optional[Publisher] = None):
        if route in self._routes:
            return self._routes[route](socket=socket, payload=payload, composite_metadata=composite_metadata)
