import warnings

from rsocket.extensions.helpers import (composite, route, authenticate_bearer,
                                        authenticate_simple, data_mime_type, data_mime_types,
                                        require_route)

warnings.warn("use rsocket.extensions.helpers instead", DeprecationWarning,
              stacklevel=2)

__all__ = [
    'composite',
    'route',
    'authenticate_simple',
    'authenticate_bearer',
    'data_mime_types',
    'data_mime_type',
    'require_route'
]
