import asyncio

from rsocket.acksubscriber import AsyncIOAckSubscriber


class StreamSubscriber(AsyncIOAckSubscriber):
    def __init__(self, stream: int, socket, loop: asyncio.AbstractEventLoop, initial_request_n: int):
        super().__init__(stream, socket, loop, initial_request_n)

    def on_completed(self):
        super().on_completed()
        self.socket.finish_stream(self.stream)
