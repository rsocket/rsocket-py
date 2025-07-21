from datetime import timedelta
from typing import Optional

from rsocket.frame import Frame
from rsocket.queue_peekable import QueuePeekable


class PositionedFrame:
    def __init__(self, frame: Frame, position: int):
        self.frame = frame
        self.position = position


class ResumeSession:
    def __init__(self,
                 resume_token_id: str,
                 timeout: Optional[timedelta] = None
                 ):
        self.resume_token_id = resume_token_id
        self._timeout = timeout
        self._next_position = 0

        self._frames = QueuePeekable()

    def record_frame(self, frame: Frame):
        positioned_frame = PositionedFrame(frame, self._next_position)
        self._frames.put(positioned_frame)
        self._next_position += 1

    def cleanup_until_including(self, position: int):
        while not self._frames.empty() and self._frames.peek_nowait().position <= position:
            self._frames.remove_nowait()
