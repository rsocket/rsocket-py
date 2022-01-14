from rsocket.rsocket import RSocket


class RSocketServer(RSocket):

    def _get_first_stream_id(self) -> int:
        return 2

    def _before_sender(self):
        pass

    def _finally_sender(self):
        pass

    def _update_last_keepalive(self):
        pass

    def is_server_alive(self) -> bool:
        return True
