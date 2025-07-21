from rsocket.resume.resume_interface import ResumeInterface
from rsocket.resume.resume_session import ResumeSession


class ResumeRegistry(ResumeInterface):
    def __init__(self):
        self._session_by_resume_token_id: dict[str, ResumeSession] = {}

    def register_session(self, session: ResumeSession):
        self._session_by_resume_token_id[session.resume_token_id] = session
