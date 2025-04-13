from abc import ABCMeta, abstractmethod
from datetime import timedelta
from typing import Tuple, Optional

from reactivestreams.publisher import Publisher
from reactivestreams.subscriber import Subscriber
from rsocket.error_codes import ErrorCode
from rsocket.extensions.composite_metadata import CompositeMetadata
from rsocket.local_typing import Awaitable
from rsocket.logger import logger
from rsocket.payload import Payload


class RequestHandler(metaclass=ABCMeta):
    """
    An interface which defines handler for all rsocket interactions, and some other events (e.g. on_setup).
    """

    @abstractmethod
    async def on_setup(self,
                       data_encoding: bytes,
                       metadata_encoding: bytes,
                       payload: Payload):
        ...

    @abstractmethod
    async def on_metadata_push(self, metadata: Payload):
        """
        Handle metadata-push request
        """

    @abstractmethod
    async def request_channel(self,
                              payload: Payload
                              ) -> Tuple[Optional[Publisher], Optional[Subscriber]]:
        """
        Bi-Directional communication.  A publisher on each end is connected
        to a subscriber on the other end. Note that the first payload sent to the handler is passed as
        an argument to this method and not to the local subscriber.
        """

    @abstractmethod
    async def request_fire_and_forget(self, payload: Payload):
        """
        Handle fire-and-forget request
        """

    @abstractmethod
    async def request_response(self, payload: Payload) -> Awaitable[Payload]:
        """
        Handle request-response interaction
        """

    @abstractmethod
    async def request_stream(self, payload: Payload) -> Publisher:
        """
        Handle request-stream interaction
        """

    @abstractmethod
    async def on_error(self, error_code: ErrorCode, payload: Payload):
        """
        Handle errors received from the remote side
        """

    @abstractmethod
    async def on_keepalive_timeout(self,
                                   time_since_last_keepalive: timedelta,
                                   rsocket):
        """
        Handle keepalive timeout
        """

    @abstractmethod
    async def on_connection_error(self, rsocket, exception: Exception):
        """
        Handle connection error
        """

    @abstractmethod
    async def on_close(self, rsocket, exception: Optional[Exception] = None):
        """
        Handle connection closed
        """

    # noinspection PyMethodMayBeStatic
    def _parse_composite_metadata(self, metadata: bytes) -> CompositeMetadata:
        composite_metadata = CompositeMetadata()
        composite_metadata.parse(metadata)
        return composite_metadata


class BaseRequestHandler(RequestHandler):
    """
    Default implementation of :class:`RequestHandler <rsocket.request_handler.RequestHandler>` to simplify
    implementing handlers.

    For each request handler, the implementation will raise a RuntimeError. For :meth:`request_fire_and_forget` and
    :meth:`on_metadata_push` the request will be ignored.
    """

    async def on_setup(self,
                       data_encoding: bytes,
                       metadata_encoding: bytes,
                       payload: Payload):
        """Nothing to do on setup by default"""

    async def request_channel(self, payload: Payload) -> Tuple[Optional[Publisher], Optional[Subscriber]]:
        """
        Raise RuntimeError by default if not implemented.
        """
        raise RuntimeError('Not implemented')

    async def request_fire_and_forget(self, payload: Payload):
        """Ignored by default"""

    async def on_metadata_push(self, payload: Payload):
        """Nothing by default"""

    async def request_response(self, payload: Payload) -> Awaitable[Payload]:
        raise RuntimeError('Not implemented')

    async def request_stream(self, payload: Payload) -> Publisher:
        raise RuntimeError('Not implemented')

    async def on_error(self, error_code: ErrorCode, payload: Payload):
        logger().error('Error handler: %s, %s', error_code.name, payload)

    async def on_connection_error(self, rsocket, exception: Exception):
        pass

    async def on_close(self, rsocket, exception: Optional[Exception] = None):
        pass

    async def on_keepalive_timeout(self,
                                   time_since_last_keepalive: timedelta,
                                   rsocket):
        pass
