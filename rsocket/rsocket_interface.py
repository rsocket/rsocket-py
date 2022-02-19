import abc
from typing import Union, Optional, Any

from reactivestreams.publisher import Publisher
from rsocket.frame import Frame, RequestFrame
from rsocket.payload import Payload


class RSocketInterface(metaclass=abc.ABCMeta):
    def connect(self):
        ...

    def request_channel(
            self,
            payload: Payload,
            local_publisher: Optional[Publisher] = None) -> Union[Any, Publisher]:
        ...

    def register_stream(self, handler) -> int:
        ...

    def send_frame(self, frame: Frame):
        ...

    def send_complete(self, stream_id: int):
        ...

    def finish_stream(self, stream_id: int):
        ...

    def _log_identifier(self) -> str:
        ...

    def send_request(self, frame: RequestFrame):
        ...

    def send_payload(self, stream_id: int, payload: Payload, complete=False, is_next=True):
        ...

    def send_error(self, stream_id: int, exception: Exception):
        ...
