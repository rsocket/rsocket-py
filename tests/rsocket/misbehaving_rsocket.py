from rsocket.frame import Frame
from rsocket.transports.transport import Transport


class MisbehavingRSocket:
    def __init__(self, transport: Transport):
        self._transport = transport

    async def send_frame(self, frame: Frame):
        await self._transport.send_frame(frame)


class BrokenFrame:
    def __init__(self, content: bytes):
        self._content = content

    def serialize(self) -> bytes:
        return self._content


class UnknownFrame(Frame):
    def __init__(self):
        super().__init__(34)

    def parse(self, buffer: bytes, offset: int):
        pass
