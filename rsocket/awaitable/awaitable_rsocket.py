from typing import List, Optional

from reactivestreams.publisher import Publisher
from rsocket.awaitable.collector_subscriber import CollectorSubscriber
from rsocket.frame import MAX_REQUEST_N
from rsocket.local_typing import Awaitable
from rsocket.payload import Payload
from rsocket.rsocket import RSocket


class AwaitableRSocket:

    def __init__(self, rsocket: RSocket):
        self._rsocket = rsocket

    def fire_and_forget(self, payload: Payload) -> Awaitable[None]:
        return self._rsocket.fire_and_forget(payload)

    def metadata_push(self, metadata: bytes) -> Awaitable[None]:
        return self._rsocket.metadata_push(metadata)

    async def request_response(self, payload: Payload) -> Payload:
        return await self._rsocket.request_response(payload)

    async def request_stream(self,
                             payload: Payload,
                             limit_rate=MAX_REQUEST_N) -> List[Payload]:
        subscriber = CollectorSubscriber(limit_rate)

        self._rsocket.request_stream(payload).initial_request_n(limit_rate).subscribe(subscriber)

        return await subscriber.run()

    async def request_channel(self,
                              payload: Payload,
                              publisher: Optional[Publisher] = None,
                              limit_rate=MAX_REQUEST_N) -> List[Payload]:
        subscriber = CollectorSubscriber(limit_rate)

        self._rsocket.request_channel(payload, publisher).initial_request_n(limit_rate).subscribe(subscriber)

        return await subscriber.run()

    async def __aenter__(self):
        await self._rsocket.__aenter__()
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        await self._rsocket.__aexit__(exc_type, exc_val, exc_tb)

    async def connect(self):
        return await self._rsocket.connect()

    def close(self):
        self._rsocket.close()
