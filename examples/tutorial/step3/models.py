from dataclasses import dataclass
from typing import Optional


@dataclass(frozen=True)
class Message:
    user: Optional[str] = None
    content: Optional[str] = None
    channel: Optional[str] = None


chat_session_mimetype = b'chat/session-id'
chat_filename_mimetype = b'chat/file-name'
