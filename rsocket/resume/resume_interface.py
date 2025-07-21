import abc

from rsocket.resume.resume_session import ResumeSession


class ResumeInterface(metaclass=abc.ABCMeta):
    def register_session(self, session: ResumeSession):
        ...
