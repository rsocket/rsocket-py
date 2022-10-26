from typing import Optional

from rsocket.error_codes import ErrorCode


class ParseError(ValueError):
    pass


class RSocketError(Exception):
    pass


class RSocketUnknownMimetype(RSocketError):
    def __init__(self, mimetype_id):
        self.mimetype_id = mimetype_id


class RSocketUnknownAuthType(RSocketError):
    def __init__(self, auth_type_id):
        self.auth_type_id = auth_type_id


class RSocketMimetypeTooLong(RSocketError):
    def __init__(self, mimetype):
        self.mimetype = mimetype


class RSocketUnknownFrameType(RSocketError):
    def __init__(self, frame_type_id):
        self.frame_type_id = frame_type_id


class RSocketApplicationError(RSocketError):
    pass


class RSocketStreamAllocationFailure(RSocketError):
    pass


class RSocketValueError(RSocketError):
    pass


class RSocketProtocolError(RSocketError):
    def __init__(self, error_code: ErrorCode, data: Optional[str] = None):
        self.error_code = error_code
        self.data = data

    def __str__(self) -> str:
        return 'RSocket error %s(%s): "%s"' % (self.error_code.name, self.error_code.value, self.data or '')


class RSocketStreamIdInUse(RSocketProtocolError):

    def __init__(self, stream_id: int):
        super().__init__(ErrorCode.REJECTED)
        self.stream_id = stream_id


class RSocketFrameFragmentDifferentType(RSocketError):
    pass


class RSocketTransportError(RSocketError):
    pass


class RSocketNoAvailableTransport(RSocketError):
    pass
