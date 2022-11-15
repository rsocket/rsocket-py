import json
from dataclasses import dataclass
from typing import Optional

from rsocket.frame_helpers import ensure_bytes
from rsocket.payload import Payload


@dataclass(frozen=True)
class Message:
    user: Optional[str] = None
    content: Optional[str] = None


def encode_dataclass(obj):
    return ensure_bytes(json.dumps(obj.__dict__))


def dataclass_to_payload(obj) -> Payload:
    return Payload(encode_dataclass(obj))
