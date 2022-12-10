import json
from dataclasses import dataclass
from typing import Optional, TypeVar, Type

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