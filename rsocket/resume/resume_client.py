from typing import Optional

from rsocket.resume.resume_interface import ResumeInterface
from rsocket.resume.resume_session import ResumeSession


class ResumeClient(ResumeInterface):
    def __init__(self):
        self._resume_token: Optional[str] = None
        self._session: Optional[ResumeSession] = None

    def register_session(self, session: ResumeSession):
        if self._resume_token is None:
            self._resume_token = session.resume_token_id
            self._session = session

    def get_session(self) -> Optional[ResumeSession]:
        return self._session

    def get_resume_token(self) -> Optional[str]:
        return self._resume_token
