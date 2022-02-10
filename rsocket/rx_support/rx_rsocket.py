from typing import Optional, Union

import rx
from rx import Observable
from rx.subject import Subject

from reactivestreams.publisher import Publisher
from rsocket.frame import MAX_REQUEST_N
from rsocket.payload import Payload
from rsocket.rsocket import RSocket
from rsocket.rx_support.back_pressure_publisher import BackPressurePublisher
from rsocket.rx_support.back_pressure_subscriber import BackPressureSubscriber
from rsocket.streams.backpressureapi import BackpressureApi


class RxRSocket:
    def __init__(self, rsocket: RSocket):
        self._rsocket = rsocket

    def request_stream(self, request: Payload, request_limit: int = MAX_REQUEST_N) -> Observable:
        response_publisher = self._rsocket.request_stream(request)
        return self._to_subject(response_publisher, request_limit)

    def request_response(self, request: Payload) -> Observable:
        return rx.from_future(self._rsocket.request_response(request))

    def request_channel(self,
                        request: Payload,
                        request_limit: int = MAX_REQUEST_N,
                        observable: Optional[Observable] = None) -> Observable:
        if observable is not None:
            local_publisher = BackPressurePublisher(observable)
        else:
            local_publisher = None

        response_publisher = self._rsocket.request_channel(request, local_publisher)
        return self._to_subject(response_publisher, request_limit)

    def fire_and_forget(self, request: Payload):
        self._rsocket.fire_and_forget(request)

    def metadata_push(self, metadata: bytes):
        self._rsocket.metadata_push(metadata)

    def _to_subject(self, publisher: Union[Publisher, BackpressureApi], request_limit: int) -> Subject:
        subject = Subject()
        publisher.initial_request_n(request_limit).subscribe(BackPressureSubscriber(subject, request_limit))
        return subject

    def connect(self):
        return self._rsocket.connect()

    def close(self):
        self._rsocket.close()

    async def __aenter__(self):
        await self._rsocket.__aenter__()
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        await self._rsocket.__aexit__(exc_type, exc_val, exc_tb)
