from datetime import timedelta
from typing import Callable, Optional

from rsocket.resume.resume_session import ResumeSession
from rsocket.rsocket_base import RSocketBase


class RSocketResume:
    def __init__(self,
                 session_factory: Callable[[ResumeSession], RSocketBase],
                 timeout: Optional[timedelta] = None
                 ):

        self._sessions = []
        self._timeout = timeout
        self._session_factory = session_factory

    def create_session(self):
         resume_session = ResumeSession()
         self._sessions.append(self._session_factory())
