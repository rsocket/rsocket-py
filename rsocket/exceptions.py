from typing import Optional

from rsocket.error_codes import ErrorCode


class RSocketError(Exception):
    pass


class RSocketApplicationError(RSocketError):
    pass


class RSocketAuthenticationError(RSocketError):
    pass


class RSocketValueErrorException(RSocketError):
    pass


class RSocketProtocolException(RSocketError):
    def __init__(self, error_code: ErrorCode, data: Optional[str] = None):
        self.error_code = error_code
        self.data = data


class RSocketFrameFragmentDifferentType(RSocketError):
    pass
