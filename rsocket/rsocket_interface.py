import abc
from typing import Union, Optional, Any

from reactivestreams.publisher import Publisher
from rsocket.frame import Frame, RequestFrame
from rsocket.payload import Payload


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
    def send_frame(self, frame: Frame):
        ...

    @abc.abstractmethod
    def send_complete(self, stream_id: int):
        ...

    @abc.abstractmethod
    def finish_stream(self, stream_id: int):
        ...

    @abc.abstractmethod
    def _log_identifier(self) -> str:
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
