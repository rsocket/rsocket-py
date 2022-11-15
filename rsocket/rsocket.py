import abc
import asyncio
from typing import Union, Optional, Any

from reactivestreams.publisher import Publisher
from rsocket.local_typing import Awaitable
from rsocket.payload import Payload
from rsocket.streams.backpressureapi import BackpressureApi


class RSocket(metaclass=abc.ABCMeta):

    @abc.abstractmethod
    def request_channel(
            self,
            payload: Payload,
            publisher: Optional[Publisher] = None,
            sending_done: Optional[asyncio.Event] = None) -> Union[Any, Publisher]:
        ...

    @abc.abstractmethod
    def request_response(self, payload: Payload) -> Awaitable[Payload]:
        ...

    @abc.abstractmethod
    def fire_and_forget(self, payload: Payload) -> Awaitable[None]:
        ...

    @abc.abstractmethod
    def request_stream(self, payload: Payload) -> Union[BackpressureApi, Publisher]:
        ...

    @abc.abstractmethod
    def metadata_push(self, metadata: bytes) -> Awaitable[None]:
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
