from typing import Callable, Any

from rsocket import Payload, RSocket
from rsocket.extensions.composite_metadata import CompositeMetadata

decorated_method = Callable[[RSocket, Payload, CompositeMetadata], Any]


class RequestRouter:
    __slots__ = ('_routes',)

    def __init__(self):
        self._routes = {}

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

    def __call__(self, socket, route: str, payload: Payload, composite_metadata: CompositeMetadata):
        return self._routes[route](socket, payload, composite_metadata)
