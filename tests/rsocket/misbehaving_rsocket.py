from rsocket.frame import Frame
from rsocket.transports.transport import Transport


class MisbehavingRSocket:
    def __init__(self, socket: Transport):
        self._socket = socket

    async def send_frame(self, frame: Frame):
        await self._socket.send_frame(frame)


class BrokenFrame:
    def __init__(self, content: bytes):
        self._content = content

    def serialize(self) -> bytes:
        return self._content
