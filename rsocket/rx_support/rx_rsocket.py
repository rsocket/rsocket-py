import asyncio
from typing import Optional

import rx
from rx import Observable

from rsocket.frame import MAX_REQUEST_N
from rsocket.payload import Payload
from rsocket.rsocket import RSocket
from rsocket.rx_support.back_pressure_publisher import BackPressurePublisher
from rsocket.rx_support.from_rsocket_publisher import from_rsocket_publisher


class RxRSocket:
    def __init__(self, rsocket: RSocket):
        self._rsocket = rsocket

    def request_stream(self, request: Payload, request_limit: int = MAX_REQUEST_N) -> Observable:
        response_publisher = self._rsocket.request_stream(request).initial_request_n(request_limit)
        return from_rsocket_publisher(response_publisher, request_limit)

    def request_response(self, request: Payload) -> Observable:
        return rx.from_future(self._rsocket.request_response(request))

    def request_channel(self,
                        request: Payload,
                        request_limit: int = MAX_REQUEST_N,
                        observable: Optional[Observable] = None,
                        sending_done_event: Optional[asyncio.Event] = None) -> Observable:
        if observable is not None:
            local_publisher = BackPressurePublisher(observable)
        else:
            local_publisher = None

        response_publisher = self._rsocket.request_channel(
            request, local_publisher, sending_done_event
        ).initial_request_n(request_limit)
        return from_rsocket_publisher(response_publisher, request_limit)

    def fire_and_forget(self, request: Payload) -> rx.Observable:
        return rx.from_future(self._rsocket.fire_and_forget(request))

    def metadata_push(self, metadata: bytes) -> rx.Observable:
        return rx.from_future(self._rsocket.metadata_push(metadata))

    async def connect(self):
        return await self._rsocket.connect()

    async def close(self):
        await self._rsocket.close()

    async def __aenter__(self):
        await self._rsocket.__aenter__()
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        await self._rsocket.__aexit__(exc_type, exc_val, exc_tb)
