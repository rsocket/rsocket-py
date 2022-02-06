from typing import Optional, Union

import rx
from rx import Observable
from rx.subject import Subject

from reactivestreams.publisher import Publisher
from rsocket.payload import Payload
from rsocket.rsocket_client import RSocketClient
from rsocket.rx_support.back_pressure_publisher import BackPressurePublisher
from rsocket.rx_support.back_pressure_subscriber import BackPressureSubscriber
from rsocket.streams.backpressureapi import BackpressureApi
from rsocket.streams.stream_handler import MAX_REQUEST_N


class RxRSocketClient:
    def __init__(self, rsocket_client: RSocketClient):
        self._rsocket_client = rsocket_client

    def request_stream(self, request: Payload, request_limit: int = MAX_REQUEST_N) -> Observable:
        response_publisher = self._rsocket_client.request_stream(request)
        return self._to_subject(response_publisher, request_limit)

    def request_response(self, request: Payload) -> Observable:
        return rx.from_future(self._rsocket_client.request_response(request))

    def request_channel(self,
                        request: Payload,
                        request_limit: int = MAX_REQUEST_N,
                        observable: Optional[Observable] = None) -> Observable:
        if observable is not None:
            local_publisher = BackPressurePublisher(observable)
        else:
            local_publisher = None

        response_publisher = self._rsocket_client.request_channel(request, local_publisher)
        return self._to_subject(response_publisher, request_limit)

    def fire_and_forget(self, request: Payload):
        self._rsocket_client.fire_and_forget(request)

    def metadata_push(self, metadata: bytes):
        self._rsocket_client.metadata_push(metadata)

    def _to_subject(self, publisher: Union[Publisher, BackpressureApi], request_limit: int) -> Subject:
        subject = Subject()
        publisher.initial_request_n(request_limit).subscribe(BackPressureSubscriber(subject, request_limit))
        return subject

    def connect(self):
        return self._rsocket_client.connect()

    def close(self):
        self._rsocket_client.close()

    def __aenter__(self):
        return self._rsocket_client.__aenter__()

    def __aexit__(self, exc_type, exc_val, exc_tb):
        return self._rsocket_client.__aexit__(exc_type, exc_val, exc_tb)
