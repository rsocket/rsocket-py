import asyncio
from datetime import timedelta
from typing import Tuple, Optional, Callable

from rx import operators

from reactivestreams.publisher import Publisher
from reactivestreams.subscriber import Subscriber
from rsocket.error_codes import ErrorCode
from rsocket.payload import Payload
from rsocket.request_handler import RequestHandler
from rsocket.rx_support.back_pressure_publisher import observable_to_publisher
from rsocket.rx_support.from_rsocket_publisher import RxSubscriberFromObserver
from rsocket.rx_support.rx_handler import RxHandler

__all__ = ['rx_handler_factory']


def rx_handler_factory(handler_factory: Callable[[], RxHandler]):
    """
    Wraps an Rx handler factory into a basic request handler adapter.
    """

    def create_handler():
        return RxHandlerAdapter(handler_factory())

    return create_handler


class RxHandlerAdapter(RequestHandler):

    def __init__(self, delegate: RxHandler):
        self.delegate = delegate

    async def on_setup(self, data_encoding: bytes, metadata_encoding: bytes, payload: Payload):
        await self.delegate.on_setup(data_encoding, metadata_encoding, payload)

    async def on_metadata_push(self, metadata: Payload):
        await self.on_metadata_push(metadata)

    async def request_channel(self, payload: Payload) -> Tuple[Optional[Publisher], Optional[Subscriber]]:
        rx_channel = await self.delegate.request_channel(payload)

        subscriber = None
        publisher = observable_to_publisher(rx_channel.observable)

        if rx_channel.observer is not None:
            subscriber = RxSubscriberFromObserver(rx_channel.observer,
                                                  rx_channel.limit_rate)

        return publisher, subscriber

    async def request_fire_and_forget(self, payload: Payload):
        await self.delegate.request_fire_and_forget(payload)

    async def request_response(self, payload: Payload) -> asyncio.Future:
        observable = await self.delegate.request_response(payload)
        return observable.pipe(
            operators.default_if_empty(Payload()),
            operators.to_future()
        )

    async def request_stream(self, payload: Payload) -> Publisher:
        return observable_to_publisher(await self.delegate.request_stream(payload))

    async def on_error(self, error_code: ErrorCode, payload: Payload):
        await self.delegate.on_error(error_code, payload)

    async def on_keepalive_timeout(self, time_since_last_keepalive: timedelta, rsocket):
        await self.delegate.on_keepalive_timeout(time_since_last_keepalive, rsocket)

    async def on_connection_error(self, rsocket, exception: Exception):
        await self.delegate.on_connection_error(rsocket, exception)

    async def on_close(self, rsocket, exception: Optional[Exception] = None):
        await self.delegate.on_close(rsocket, exception)
