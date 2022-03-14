from rsocket.rsocket_base import RSocketBase


class RSocketServer(RSocketBase):

    def _log_identifier(self) -> str:
        return 'server'

    def _get_first_stream_id(self) -> int:
        return 2

    def is_server_alive(self) -> bool:
        return True
