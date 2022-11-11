from dataclasses import dataclass, field
from typing import Optional, List


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
    period_seconds: Optional[int] = field(default_factory=lambda: 5)


@dataclass(frozen=True)
class ClientStatistics:
    memory_usage: Optional[float] = None


chat_filename_mimetype = b'chat/file-name'
