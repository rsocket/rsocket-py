import asyncio
from abc import ABCMeta, abstractmethod
from asyncio import Future

from reactivestreams.publisher import Publisher, DefaultPublisher
from rsocket.extensions.composite_metadata import CompositeMetadata
from rsocket.payload import Payload


class RequestHandler(metaclass=ABCMeta):
    """
    An ABC for request handlers.
    """

    def __init__(self, socket):
        super().__init__()
        self.socket = socket

    @abstractmethod
    def on_setup(self, data_encoding: bytes, metadata_encoding: bytes):
        ...

    @abstractmethod
    def on_metadata_push(self, metadata: bytes):
        ...

    @abstractmethod
    def request_channel(self, payload: Payload, publisher: Publisher) \
            -> Publisher:
        """
        Bi-Directional communication.  A publisher on each end is connected
        to a subscriber on the other end.
        """

    @abstractmethod
    def request_fire_and_forget(self, payload: Payload):
        ...

    @abstractmethod
    def request_response(self, payload: Payload) -> asyncio.Future:
        ...

    @abstractmethod
    def request_stream(self, payload: Payload) -> Publisher:
        ...

    def _parse_composite_metadata(self, metadata: bytes) -> CompositeMetadata:
        composite_metadata = CompositeMetadata()
        composite_metadata.parse(metadata)
        return composite_metadata

    def _send_error(self, exception: Exception):
        self.socket.send_error(exception)


class BaseRequestHandler(RequestHandler):
    def on_setup(self, data_encoding: bytes, metadata_encoding: bytes):
        """Nothing to do on setup by default"""

    def request_channel(self, payload: Payload, publisher: Publisher):
        self._send_error(RuntimeError("Not implemented"))

    def request_fire_and_forget(self, payload: Payload):
        """The requester isn't listening for errors.  Nothing to do."""

    def on_metadata_push(self, metadata: bytes):
        """Nothing by default"""

    def request_response(self, payload: Payload) -> Future:
        future = asyncio.Future()
        future.set_exception(RuntimeError("Not implemented"))
        return future

    def request_stream(self, payload: Payload) -> Publisher:
        return DefaultPublisher()


