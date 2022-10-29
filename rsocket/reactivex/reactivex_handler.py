from abc import abstractmethod
from datetime import timedelta

import reactivex
from reactivex import Observable

from rsocket.error_codes import ErrorCode
from rsocket.extensions.composite_metadata import CompositeMetadata
from rsocket.helpers import create_error_future
from rsocket.logger import logger
from rsocket.payload import Payload
from rsocket.reactivex.reactivex_channel import ReactivexChannel


class ReactivexHandler:

    @abstractmethod
    async def on_setup(self,
                       data_encoding: bytes,
                       metadata_encoding: bytes,
                       payload: Payload):
        ...

    @abstractmethod
    async def on_metadata_push(self, metadata: Payload):
        ...

    @abstractmethod
    async def request_channel(self, payload: Payload) -> ReactivexChannel:
        ...

    @abstractmethod
    async def request_fire_and_forget(self, payload: Payload):
        ...

    @abstractmethod
    async def request_response(self, payload: Payload) -> Observable:
        ...

    @abstractmethod
    async def request_stream(self, payload: Payload) -> Observable:
        ...

    @abstractmethod
    async def on_error(self, error_code: ErrorCode, payload: Payload):
        ...

    @abstractmethod
    async def on_keepalive_timeout(self,
                                   time_since_last_keepalive: timedelta,
                                   rsocket):
        ...

    @abstractmethod
    async def on_connection_lost(self, rsocket, exception):
        ...

    # noinspection PyMethodMayBeStatic
    def _parse_composite_metadata(self, metadata: bytes) -> CompositeMetadata:
        composite_metadata = CompositeMetadata()
        composite_metadata.parse(metadata)
        return composite_metadata


class BaseReactivexHandler(ReactivexHandler):
    async def on_setup(self, data_encoding: bytes, metadata_encoding: bytes, payload: Payload):
        """Nothing to do on setup by default"""

    async def on_metadata_push(self, metadata: Payload):
        """Nothing by default"""

    async def request_channel(self, payload: Payload) -> ReactivexChannel:
        raise RuntimeError('Not implemented')

    async def request_fire_and_forget(self, payload: Payload):
        """The requester isn't listening for errors.  Nothing to do."""

    async def request_response(self, payload: Payload) -> Observable:
        return reactivex.from_future(create_error_future(RuntimeError('Not implemented')))

    async def request_stream(self, payload: Payload) -> Observable:
        raise RuntimeError('Not implemented')

    async def on_error(self, error_code: ErrorCode, payload: Payload):
        logger().error('Error handler: %s, %s', error_code.name, payload)

    async def on_keepalive_timeout(self, time_since_last_keepalive: timedelta, rsocket):
        pass

    async def on_connection_lost(self, rsocket, exception):
        await rsocket.close()
