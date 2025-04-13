from abc import abstractmethod
from datetime import timedelta
from typing import Optional, Union, Callable

from rx import Observable
from rx.core.typing import Subject

from rsocket.error_codes import ErrorCode
from rsocket.extensions.composite_metadata import CompositeMetadata
from rsocket.logger import logger
from rsocket.payload import Payload
from rsocket.rx_support.rx_channel import RxChannel

__all__ = ['RxHandler']


class RxHandler:
    """
    Variant of :class:`RequestHandler <rsocket.request_handler.RequestHandler>` which uses Rx (3.0).
    Wrap with :meth:`rx_handler_factory <rsocket.rx_support.rx_handler_adapter.rx_handler_factory>` to pass as a request handler
    """

    @abstractmethod
    async def on_setup(self,
                       data_encoding: bytes,
                       metadata_encoding: bytes,
                       payload: Payload):
        """
        Handle setup request
        """

    @abstractmethod
    async def on_metadata_push(self, metadata: Payload):
        """
        Handle metadata-push request
        """

    @abstractmethod
    async def request_channel(self, payload: Payload) -> RxChannel:
        """
        Handle request-channel interaction
        """

    @abstractmethod
    async def request_fire_and_forget(self, payload: Payload):
        """
        Handle request-fire-and-forget request
        """

    @abstractmethod
    async def request_response(self, payload: Payload) -> Observable:
        """
        Handle request-response interaction
        """

    @abstractmethod
    async def request_stream(self, payload: Payload) -> Union[Observable, Callable[[Subject], Observable]]:
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
        ...

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


class BaseRxHandler(RxHandler):

    async def on_setup(self, data_encoding: bytes, metadata_encoding: bytes, payload: Payload):
        """Nothing to do on setup by default"""

    async def on_metadata_push(self, metadata: Payload):
        """Nothing by default"""

    async def request_channel(self, payload: Payload) -> RxChannel:
        raise RuntimeError('Not implemented')

    async def request_fire_and_forget(self, payload: Payload):
        """The requester isn't listening for errors.  Nothing to do."""

    async def request_response(self, payload: Payload) -> Observable:
        raise RuntimeError('Not implemented')

    async def request_stream(self, payload: Payload) -> Observable:
        raise RuntimeError('Not implemented')

    async def on_error(self, error_code: ErrorCode, payload: Payload):
        logger().error('Error handler: %s, %s', error_code.name, payload)

    async def on_keepalive_timeout(self, time_since_last_keepalive: timedelta, rsocket):
        pass

    async def on_close(self, rsocket, exception: Optional[Exception] = None):
        pass

    async def on_connection_error(self, rsocket, exception: Exception):
        pass
