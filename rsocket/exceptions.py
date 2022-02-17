from typing import Optional

from rsocket.error_codes import ErrorCode


class ParseError(ValueError):
    pass


class RSocketError(Exception):
    pass


class RSocketUnknownMimetype(RSocketError):
    def __init__(self, mimetype_id):
        self.mimetype_id = mimetype_id


class RSocketUnknownFrameType(RSocketError):
    def __init__(self, frame_type_id):
        self.frame_type_id = frame_type_id


class RSocketApplicationError(RSocketError):
    pass


class RSocketStreamAllocationFailure(RSocketError):
    pass


class RSocketValueErrorException(RSocketError):
    pass


class RSocketProtocolException(RSocketError):
    def __init__(self, error_code: ErrorCode, data: Optional[str] = None):
        self.error_code = error_code
        self.data = data

    def __str__(self) -> str:
        return 'RSocket error %s(%s): "%s"' % (self.error_code.name, self.error_code.value, self.data or '')


class RSocketFrameFragmentDifferentType(RSocketError):
    pass
