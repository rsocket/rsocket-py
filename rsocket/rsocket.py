import abc
from asyncio import Future
from typing import Union, Optional, Any

from reactivestreams.publisher import Publisher
from rsocket.payload import Payload
from rsocket.streams.backpressureapi import BackpressureApi


class RSocket(metaclass=abc.ABCMeta):

    @abc.abstractmethod
    def request_channel(
            self,
            payload: Payload,
            local_publisher: Optional[Publisher] = None) -> Union[Any, Publisher]:
        ...

    @abc.abstractmethod
    def request_response(self, payload: Payload) -> Future:
        ...

    @abc.abstractmethod
    def fire_and_forget(self, payload: Payload):
        ...

    @abc.abstractmethod
    def request_stream(self, payload: Payload) -> Union[BackpressureApi, Publisher]:
        ...

    @abc.abstractmethod
    def metadata_push(self, metadata: bytes):
        ...

    @abc.abstractmethod
    async def connect(self):
        ...

    @abc.abstractmethod
    async def close(self):
        ...

    @abc.abstractmethod
    async def __aenter__(self) -> 'RSocket':
        ...

    @abc.abstractmethod
    async def __aexit__(self, exc_type, exc_val, exc_tb):
        ...
