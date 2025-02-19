import asyncio
from datetime import timedelta
from typing import Tuple, Optional, Callable

from reactivex import operators

from reactivestreams.publisher import Publisher
from reactivestreams.subscriber import Subscriber
from rsocket.error_codes import ErrorCode
from rsocket.payload import Payload
from rsocket.reactivex.back_pressure_publisher import observable_to_publisher
from rsocket.reactivex.from_rsocket_publisher import RxSubscriberFromObserver
from rsocket.reactivex.reactivex_handler import ReactivexHandler
from rsocket.request_handler import RequestHandler


def reactivex_handler_factory(handler_factory: Callable[[], ReactivexHandler]):
    """
    Wraps a reactivex handler factory into a basic request handler adapter.
    """
    def create_handler():
        return ReactivexHandlerAdapter(handler_factory())

    return create_handler


class ReactivexHandlerAdapter(RequestHandler):

    def __init__(self, delegate: ReactivexHandler):
        self.delegate = delegate

    async def on_setup(self, data_encoding: bytes, metadata_encoding: bytes, payload: Payload):
        await self.delegate.on_setup(data_encoding, metadata_encoding, payload)

    async def on_metadata_push(self, metadata: Payload):
        await self.on_metadata_push(metadata)

    async def request_channel(self, payload: Payload) -> Tuple[Optional[Publisher], Optional[Subscriber]]:
        reactivex_channel = await self.delegate.request_channel(payload)

        subscriber = None
        publisher = observable_to_publisher(reactivex_channel.observable)

        if reactivex_channel.observer is not None:
            subscriber = RxSubscriberFromObserver(reactivex_channel.observer,
                                                  reactivex_channel.limit_rate)

        return publisher, subscriber

    async def request_fire_and_forget(self, payload: Payload):
        await self.delegate.request_fire_and_forget(payload)

    async def request_response(self, payload: Payload) -> asyncio.Future:
        response = await self.delegate.request_response(payload)

        if isinstance(response, asyncio.Future):
            observable = await response
        else:
            observable = response

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
