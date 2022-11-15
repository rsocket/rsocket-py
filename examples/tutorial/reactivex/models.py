import json
from dataclasses import dataclass, field
from typing import Optional, List

from rsocket.frame_helpers import ensure_bytes
from rsocket.payload import Payload


@dataclass(frozen=True)
class Message:
    user: Optional[str] = None
    content: Optional[str] = None
    channel: Optional[str] = None


@dataclass(frozen=True)
class ServerStatistics:
    user_count: Optional[int] = None
    channel_count: Optional[int] = None


@dataclass()
class ServerStatisticsRequest:
    ids: Optional[List[str]] = field(default_factory=lambda: ['users', 'channels'])
    period_seconds: Optional[int] = field(default_factory=lambda: 2)


@dataclass(frozen=True)
class ClientStatistics:
    memory_usage: Optional[float] = None


chat_filename_mimetype = b'chat/file-name'


def encode_dataclass(obj) -> bytes:
    return ensure_bytes(json.dumps(obj.__dict__))


def dataclass_to_payload(obj) -> Payload:
    return Payload(encode_dataclass(obj))
