class RSocketError(Exception):
    pass


class RSocketApplicationError(RSocketError):
    pass


class RSocketAuthenticationError(RSocketError):
    pass


class RSocketProtocolException(RSocketError):
    pass


class RSocketFrameFragmentDifferentType(RSocketError):
    pass
