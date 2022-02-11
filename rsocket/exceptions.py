from typing import Optional

from rsocket.error_codes import ErrorCode


class RSocketError(Exception):
    pass


class RSocketApplicationError(RSocketError):
    pass


class RSocketStreamAllocationFailure(RSocketError):
    pass


class RSocketAuthenticationError(RSocketError):
    pass


class RSocketLeaseNotImplementedError(RSocketError):
    pass


class RSocketLeaseNotReceivedTimeoutError(RSocketError):
    pass


class RSocketValueErrorException(RSocketError):
    pass


class RSocketConnectionRejected(RSocketError):
    pass


class RSocketProtocolException(RSocketError):
    def __init__(self, error_code: ErrorCode, data: Optional[str] = None):
        self.error_code = error_code
        self.data = data

    def __str__(self) -> str:
        return 'RSocket error %s(%s): "%s"' % (self.error_code.name, self.error_code.value, self.data or '')


class RSocketRejected(RSocketProtocolException):
    def __init__(self, stream_id: Optional[int] = None):
        super().__init__(ErrorCode.REJECTED)
        self.stream_id = stream_id


class RSocketFrameFragmentDifferentType(RSocketError):
    pass
