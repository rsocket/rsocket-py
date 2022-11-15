import asyncio
from typing import Union, Optional, Any

from reactivestreams.publisher import Publisher
from rsocket.load_balancer.load_balancer_strategy import LoadBalancerStrategy
from rsocket.local_typing import Awaitable
from rsocket.payload import Payload
from rsocket.rsocket import RSocket
from rsocket.streams.backpressureapi import BackpressureApi


class LoadBalancerRSocket(RSocket):

    def __init__(self, strategy: LoadBalancerStrategy):
        self._strategy = strategy

    def request_channel(self,
                        payload: Payload,
                        publisher: Optional[Publisher] = None,
                        sending_done: Optional[asyncio.Event] = None) -> Union[Any, Publisher]:
        return self._select_client().request_channel(
            payload, publisher, sending_done
        )

    def request_response(self, payload: Payload) -> Awaitable[Payload]:
        return self._select_client().request_response(payload)

    def fire_and_forget(self, payload: Payload) -> Awaitable[None]:
        return self._select_client().fire_and_forget(payload)

    def request_stream(self, payload: Payload) -> Union[BackpressureApi, Publisher]:
        return self._select_client().request_stream(payload)

    def metadata_push(self, metadata: bytes) -> Awaitable[None]:
        return self._select_client().metadata_push(metadata)

    async def connect(self):
        await self._strategy.connect()

    async def close(self):
        await self._strategy.close()

    async def __aenter__(self) -> RSocket:
        await self._strategy.connect()
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        await self._strategy.close()

    def _select_client(self):
        return self._strategy.select()
