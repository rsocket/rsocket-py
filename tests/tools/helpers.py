from typing import Tuple

from rsocket.rsocket_client import RSocketClient
from rsocket.rsocket_server import RSocketServer


def get_components(pipe) -> Tuple[RSocketServer, RSocketClient]:
    return pipe
