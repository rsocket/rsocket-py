class RSocketError(Exception):
    pass


class RSocketApplicationError(RSocketError):
    pass


class RsocketAuthenticationError(RSocketError):
    pass
