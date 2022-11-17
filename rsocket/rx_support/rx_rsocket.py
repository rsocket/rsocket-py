import asyncio
from asyncio import Future
from typing import Optional, cast, Union, Callable

import rx
from rx import Observable, operators
from rx.core.typing import Subject

from rsocket.frame import MAX_REQUEST_N
from rsocket.helpers import is_non_empty_payload
from rsocket.payload import Payload
from rsocket.rx_support.back_pressure_publisher import observable_to_publisher
from rsocket.rsocket import RSocket
from rsocket.rx_support.from_rsocket_publisher import from_rsocket_publisher


class RxRSocket:
    def __init__(self, rsocket: RSocket):
        self._rsocket = rsocket

    def request_stream(self, request: Payload, request_limit: int = MAX_REQUEST_N) -> Observable:
        response_publisher = self._rsocket.request_stream(request).initial_request_n(request_limit)
        return from_rsocket_publisher(response_publisher, request_limit)

    def request_response(self, request: Payload) -> Observable:
        return rx.from_future(cast(Future, self._rsocket.request_response(request))).pipe(
            operators.filter(is_non_empty_payload)
        )

    def request_channel(self,
                        request: Payload,
                        request_limit: int = MAX_REQUEST_N,
                        observable: Optional[Union[Observable, Callable[[Subject], Observable]]] = None,
                        sending_done: Optional[asyncio.Event] = None) -> Observable:
        requester_publisher = observable_to_publisher(observable)

        response_publisher = self._rsocket.request_channel(
            request, requester_publisher, sending_done
        ).initial_request_n(request_limit)
        return from_rsocket_publisher(response_publisher, request_limit)

    def fire_and_forget(self, request: Payload) -> Observable:
        return rx.from_future(cast(Future, self._rsocket.fire_and_forget(request)))

    def metadata_push(self, metadata: bytes) -> Observable:
        return rx.from_future(cast(Future, self._rsocket.metadata_push(metadata)))

    async def connect(self):
        return await self._rsocket.connect()

    async def close(self):
        await self._rsocket.close()

    async def __aenter__(self):
        await self._rsocket.__aenter__()
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        await self._rsocket.__aexit__(exc_type, exc_val, exc_tb)
