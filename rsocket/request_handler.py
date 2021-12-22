import asyncio
from abc import ABCMeta, abstractmethod

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
    async def on_setup(self, data_encoding: bytes, metadata_encoding: bytes):
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


class BaseRequestHandler(RequestHandler):
    async def on_setup(self, data_encoding: bytes, metadata_encoding: bytes):
        """Nothing to do on setup by default"""

    def request_channel(self, payload: Payload, publisher: Publisher):
        self.socket.send_error(RuntimeError("Not implemented"))

    def request_fire_and_forget(self, payload: Payload):
        """The requester isn't listening for errors.  Nothing to do."""

    def request_response(self, payload: Payload):
        future = asyncio.Future()
        future.set_exception(RuntimeError("Not implemented"))
        return future

    def request_stream(self, payload: Payload) -> Publisher:
        return DefaultPublisher()
