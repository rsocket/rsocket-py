import asyncio
from datetime import timedelta
from typing import Tuple, Optional, Type

from rx import operators

from reactivestreams.publisher import Publisher
from reactivestreams.subscriber import Subscriber
from rsocket.error_codes import ErrorCode
from rsocket.payload import Payload
from rsocket.rx_support.back_pressure_publisher import BackPressurePublisher
from rsocket.rx_support.from_rsocket_publisher import RxSubscriberFromObserver
from rsocket.rx_support.rx_handler import RxHandler
from rsocket.request_handler import RequestHandler
from rsocket.rsocket import RSocket


def rx_handler_factory(handler_factory: Type[RxHandler]):
    def create_handler(socket: RSocket):
        return RxHandlerAdapter(handler_factory(socket), socket)

    return create_handler


class RxHandlerAdapter(RequestHandler):

    def __init__(self, delegate: RxHandler, socket: RSocket):
        super().__init__(socket)
        self.delegate = delegate

    async def on_setup(self, data_encoding: bytes, metadata_encoding: bytes, payload: Payload):
        await self.delegate.on_setup(data_encoding, metadata_encoding, payload)

    async def on_metadata_push(self, metadata: Payload):
        await self.on_metadata_push(metadata)

    async def request_channel(self, payload: Payload) -> Tuple[Optional[Publisher], Optional[Subscriber]]:
        rx_channel = await self.delegate.request_channel(payload)

        publisher = None
        subscriber = None
        if rx_channel.observable is not None:
            publisher = BackPressurePublisher(rx_channel.observable)

        if rx_channel.observer is not None:
            subscriber = RxSubscriberFromObserver(rx_channel.observer,
                                                  rx_channel.limit_rate)

        return publisher, subscriber

    async def request_fire_and_forget(self, payload: Payload):
        await self.delegate.request_fire_and_forget(payload)

    async def request_response(self, payload: Payload) -> asyncio.Future:
        observable = await self.delegate.request_response(payload)
        return observable.pipe(operators.to_future())

    async def request_stream(self, payload: Payload) -> Publisher:
        return BackPressurePublisher(await self.delegate.request_stream(payload))

    async def on_error(self, error_code: ErrorCode, payload: Payload):
        await self.delegate.on_error(error_code, payload)

    async def on_keepalive_timeout(self, time_since_last_keepalive: timedelta, rsocket):
        await self.delegate.on_keepalive_timeout(time_since_last_keepalive, rsocket)

    async def on_connection_lost(self, rsocket, exception):
        await self.delegate.on_connection_lost(rsocket, exception)
