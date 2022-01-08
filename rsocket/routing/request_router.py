from typing import Callable, Any, Optional

from reactivestreams.publisher import Publisher
from rsocket import Payload, RSocket
from rsocket.extensions.composite_metadata import CompositeMetadata

decorated_method = Callable[[RSocket, Payload, CompositeMetadata], Any]
channel_decorated_method = Callable[[RSocket, Payload, CompositeMetadata], Any]


class RequestRouter:
    __slots__ = ('_routes', '_channel_routes')

    def __init__(self):
        self._routes = {}
        self._channel_routes = {}

    def response(self, route: str):
        def decorator(function: decorated_method):
            self._routes[route] = function
            return function

        return decorator

    def stream(self, route: str):
        def decorator(function: decorated_method):
            self._routes[route] = function
            return function

        return decorator

    def channel(self, route: str):
        def decorator(function: channel_decorated_method):
            self._channel_routes[route] = function
            return function

        return decorator

    def fire_and_forget(self, route: str):
        def decorator(function: decorated_method):
            self._routes[route] = function
            return function

        return decorator

    def __call__(self,
                 socket,
                 route: str,
                 payload: Payload,
                 composite_metadata: CompositeMetadata,
                 publisher: Optional[Publisher] = None):
        if route in self._routes:
            return self._routes[route](socket, payload, composite_metadata)

        if route in self._channel_routes:
            return self._channel_routes[route](socket, payload, composite_metadata, publisher)
