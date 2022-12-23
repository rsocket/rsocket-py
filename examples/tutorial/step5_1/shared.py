import json
from dataclasses import dataclass
from pathlib import Path
from typing import Optional, TypeVar, Type

from reactivestreams.publisher import DefaultPublisher
from reactivestreams.subscriber import DefaultSubscriber, Subscriber
from reactivestreams.subscription import Subscription, DefaultSubscription
from rsocket.frame_helpers import ensure_bytes
from rsocket.helpers import utf8_decode
from rsocket.payload import Payload


@dataclass(frozen=True)
class Message:
    user: Optional[str] = None
    content: Optional[str] = None
    channel: Optional[str] = None


chat_filename_mimetype = b'chat/file-name'


def encode_dataclass(obj):
    return ensure_bytes(json.dumps(obj.__dict__))


def dataclass_to_payload(obj) -> Payload:
    return Payload(encode_dataclass(obj))


T = TypeVar('T')


def decode_dataclass(data: bytes, cls: Type[T]) -> T:
    return cls(**json.loads(utf8_decode(data)))


def decode_payload(cls, payload: Payload):
    data = payload.data

    if cls is bytes:
        return data
    if cls is str:
        return utf8_decode(data)

    return decode_dataclass(data, cls)


class FileSender(DefaultPublisher, DefaultSubscription):

    def __init__(self, file_path: Path):
        super().__init__()
        self._file_path = file_path
        self._file_handle = None

    def subscribe(self, subscriber: Subscriber):
        super().subscribe(subscriber)
        self._file_handle = self._file_path.open()

    def request(self, n: int):
        super().request(n)

    def cancel(self):
        super().cancel()


class FileReceiver(DefaultSubscriber):

    def __init__(self):
        super().__init__()
        self._filename: Optional[str] = None
        self._file_handle = None

    def on_next(self, value, is_complete=False):
        super().on_next(value, is_complete)

    def on_error(self, exception: Exception):
        super().on_error(exception)

    def on_subscribe(self, subscription: Subscription):
        super().on_subscribe(subscription)

    def on_complete(self):
        super().on_complete()
