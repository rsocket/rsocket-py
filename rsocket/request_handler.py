import asyncio
from abc import ABCMeta, abstractmethod
from asyncio import Future
from typing import Union

from reactivestreams.publisher import Publisher, DefaultPublisher
from reactivestreams.subscriber import Subscriber
from reactivestreams.subscription import Subscription
from rsocket.extensions.composite_metadata import CompositeMetadata
from rsocket.frame import LeaseFrame
from rsocket.payload import Payload


class RequestHandler(metaclass=ABCMeta):

    def __init__(self, socket):
        super().__init__()
        self.socket = socket

    @abstractmethod
    async def on_setup(self,
                       data_encoding: bytes,
                       metadata_encoding: bytes):
        ...

    async def supply_lease(self):
        """Not implemented by default"""

    @abstractmethod
    async def on_metadata_push(self, metadata: bytes):
        ...

    @abstractmethod
    async def request_channel(self,
                              payload: Payload
                              ) -> Union[Publisher, Subscription, Subscriber]:
        """
        Bi-Directional communication.  A publisher on each end is connected
        to a subscriber on the other end.
        """

    @abstractmethod
    async def request_fire_and_forget(self, payload: Payload):
        ...

    @abstractmethod
    async def request_response(self, payload: Payload) -> asyncio.Future:
        ...

    @abstractmethod
    async def request_stream(self, payload: Payload) -> Publisher:
        ...

    def _parse_composite_metadata(self, metadata: bytes) -> CompositeMetadata:
        composite_metadata = CompositeMetadata()
        composite_metadata.parse(metadata)
        return composite_metadata

    def _send_lease(self, stream: int, time_to_live: int, number_of_requests: int):
        lease = LeaseFrame()
        lease.stream_id = stream
        lease.time_to_live = time_to_live
        lease.number_of_requests = number_of_requests
        self.socket.send_frame(lease)


class BaseRequestHandler(RequestHandler):
    async def on_setup(self,
                       data_encoding: bytes,
                       metadata_encoding: bytes):
        """Nothing to do on setup by default"""

    async def request_channel(
            self, payload: Payload
    ) -> Union[Publisher, Subscription, Subscriber]:
        raise RuntimeError("Not implemented")

    async def request_fire_and_forget(self, payload: Payload):
        """The requester isn't listening for errors.  Nothing to do."""

    async def on_metadata_push(self, metadata: bytes):
        """Nothing by default"""

    async def request_response(self, payload: Payload) -> Future:
        future = asyncio.Future()
        future.set_exception(RuntimeError("Not implemented"))
        return future

    async def request_stream(self, payload: Payload) -> Publisher:
        return DefaultPublisher()
