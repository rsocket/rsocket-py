import asyncio
from typing import List

from rsocket.load_balancer.load_balancer_strategy import LoadBalancerStrategy
from rsocket.rsocket import RSocket


class LoadBalancerRoundRobin(LoadBalancerStrategy):
    def __init__(self,
                 pool: List[RSocket],
                 auto_connect=True,
                 auto_close=True):
        self._auto_close = auto_close
        self._auto_connect = auto_connect
        self._pool = pool
        self._current_index = 0

    def select(self) -> RSocket:
        client = self._pool[self._current_index]
        self._current_index = (self._current_index + 1) % len(self._pool)
        return client

    async def connect(self):
        if self._auto_connect:
            [await client.connect() for client in self._pool]

    async def close(self):
        if self._auto_close:
            await asyncio.gather(*[client.close() for client in self._pool],
                                 return_exceptions=True)
