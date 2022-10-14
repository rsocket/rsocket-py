import abc
from typing import Optional

from rsocket.error_codes import ErrorCode
from rsocket.frame import Frame, RequestFrame
from rsocket.payload import Payload


class RSocketInternal(metaclass=abc.ABCMeta):
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

    @abc.abstractmethod
    def stop_all_streams(self, error_code=ErrorCode.CANCELED, data=b''):
        ...

    @abc.abstractmethod
    def get_fragment_size_bytes(self) -> Optional[int]:
        ...
