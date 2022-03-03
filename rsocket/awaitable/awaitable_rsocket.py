from typing import List, Optional

from reactivestreams.publisher import Publisher
from rsocket.awaitable.collector_subscriber import CollectorSubscriber
from rsocket.frame import MAX_REQUEST_N
from rsocket.payload import Payload
from rsocket.rsocket import RSocket


class AwaitableRSocket:
    def __init__(self, rsocket: RSocket):
        self._rsocket = rsocket

    async def request_response(self, payload: Payload) -> Payload:
        return await self._rsocket.request_response(payload)

    async def request_stream(self,
                             payload: Payload,
                             initial_request_n=MAX_REQUEST_N) -> List[Payload]:
        subscriber = CollectorSubscriber()

        self._rsocket.request_stream(payload).initial_request_n(initial_request_n).subscribe(subscriber)

        return await subscriber.run()

    async def request_channel(self,
                              payload: Payload,
                              publisher: Optional[Publisher] = None,
                              initial_request_n=MAX_REQUEST_N) -> List[Payload]:
        subscriber = CollectorSubscriber()

        self._rsocket.request_channel(payload, publisher).initial_request_n(initial_request_n).subscribe(subscriber)

        return await subscriber.run()
