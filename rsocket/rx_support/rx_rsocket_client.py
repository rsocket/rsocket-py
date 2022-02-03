from asyncio import Future
from typing import Optional

from rx import Observable
from rx.subject import Subject

from reactivestreams.publisher import Publisher
from rsocket.payload import Payload
from rsocket.rsocket_client import RSocketClient
from rsocket.rx_support.rx_subscriber import RxSubscriber


class RxRSocketClient:
    def __init__(self, rsocket_client: RSocketClient):
        self._rsocket_client = rsocket_client

    def request_stream(self, request: Payload) -> Observable:
        return self._to_subject(self._rsocket_client.request_stream(request))

    def request_response(self, request: Payload) -> Future:
        return self._rsocket_client.request_response(request)

    def request_channel(self, request: Payload, publisher: Optional[Publisher] = None) -> Observable:
        return self._to_subject(self._rsocket_client.request_channel(request, publisher))

    def fire_and_forget(self, request: Payload):
        self._rsocket_client.fire_and_forget(request)

    def metadata_push(self, metadata: bytes):
        self._rsocket_client.metadata_push(metadata)

    def _to_subject(self, publisher: Publisher) -> Subject:
        subject = Subject()
        publisher.subscribe(RxSubscriber(subject))
        return subject

    def connect(self):
        return self._rsocket_client.connect()

    def close(self):
        self._rsocket_client.close()

    def __aenter__(self):
        return self._rsocket_client.__aenter__()

    def __aexit__(self, exc_type, exc_val, exc_tb):
        return self._rsocket_client.__aexit__(exc_type, exc_val, exc_tb)
