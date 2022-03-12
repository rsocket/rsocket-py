import abc
from asyncio import Future
from typing import Union, Optional, Any

from reactivestreams.publisher import Publisher
from rsocket.frame import Frame, RequestFrame
from rsocket.payload import Payload
from rsocket.streams.backpressureapi import BackpressureApi


class RSocketInterface(metaclass=abc.ABCMeta):

    @abc.abstractmethod
    def connect(self):
        ...

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
    def send_frame(self, frame: Frame):
        ...

    @abc.abstractmethod
    def send_complete(self, stream_id: int):
        ...

    @abc.abstractmethod
    def finish_stream(self, stream_id: int):
        ...

    @abc.abstractmethod
    def send_request(self, frame: RequestFrame):
        ...

    @abc.abstractmethod
    def send_payload(self, stream_id: int, payload: Payload, complete=False, is_next=True):
        ...

    @abc.abstractmethod
    def send_error(self, stream_id: int, exception: Exception):
        ...
