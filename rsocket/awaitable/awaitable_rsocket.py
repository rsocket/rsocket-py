from typing import List, Optional

from reactivestreams.publisher import Publisher
from rsocket.awaitable.collector_subscriber import CollectorSubscriber
from rsocket.payload import Payload
from rsocket.rsocket import RSocket


class AwaitableRSocket:
    def __init__(self, rsocket: RSocket):
        self._rsocket = rsocket

    async def request_response(self, payload: Payload) -> Payload:
        return await self._rsocket.request_response(payload)

    async def request_stream(self, payload: Payload) -> List[Payload]:
        subscriber = CollectorSubscriber()

        self._rsocket.request_stream(payload).subscribe(subscriber)

        return await subscriber.run()

    async def request_channel(self, payload: Payload, publisher: Optional[Publisher] = None) -> List[Payload]:
        subscriber = CollectorSubscriber()

        self._rsocket.request_channel(payload, publisher).subscribe(subscriber)

        return await subscriber.run()
