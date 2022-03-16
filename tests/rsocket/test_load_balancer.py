import asyncio
from contextlib import AsyncExitStack
from typing import Tuple, Optional

from reactivestreams.publisher import Publisher
from reactivestreams.subscriber import Subscriber
from rsocket.awaitable.awaitable_rsocket import AwaitableRSocket
from rsocket.helpers import create_future
from rsocket.load_balancer.load_balancer_rsocket import LoadBalancerRSocket
from rsocket.load_balancer.random_client import LoadBalancerRandom
from rsocket.load_balancer.round_robin import LoadBalancerRoundRobin
from rsocket.payload import Payload
from rsocket.streams.stream_from_generator import StreamFromGenerator
from tests.conftest import pipe_factory_tcp
from tests.rsocket.helpers import IdentifiedHandlerFactory, IdentifiedHandler, \
    to_test_response_payload


def to_response_payload(payload, server_id):
    return to_test_response_payload(
        Payload(payload.data + (' server %d' % server_id).encode(),
                payload.metadata))


class Handler(IdentifiedHandler):
    async def request_stream(self, payload: Payload) -> Publisher:
        return StreamFromGenerator(
            lambda: map(lambda _: (_, False), [to_response_payload(payload, self._server_id)]))

    async def request_fire_and_forget(self, payload: Payload):
        ...

    async def request_channel(self, payload: Payload) -> Tuple[Optional[Publisher], Optional[Subscriber]]:
        return StreamFromGenerator(lambda: map(Payload, [b'1', b'2'])), None

    async def request_response(self, payload: Payload):
        return create_future(to_response_payload(payload, self._server_id))


async def test_load_balancer_round_robin_request_response(unused_tcp_port_factory):
    clients = []
    server_count = 3
    request_count = 7

    async with AsyncExitStack() as stack:
        for i in range(server_count):
            tcp_port = unused_tcp_port_factory()
            _, client = await stack.enter_async_context(
                pipe_factory_tcp(tcp_port,
                                 server_arguments={'handler_factory': IdentifiedHandlerFactory(i, Handler).factory},
                                 auto_connect_client=False))
            clients.append(client)

        round_robin = LoadBalancerRoundRobin(clients)
        async with LoadBalancerRSocket(round_robin) as load_balancer_client:
            results = await asyncio.gather(
                *[load_balancer_client.request_response(Payload(('request %d' % j).encode()))
                  for j in range(request_count)]
            )

            assert results[0].data == b'data: request 0 server 0'
            assert results[1].data == b'data: request 1 server 1'
            assert results[2].data == b'data: request 2 server 2'
            assert results[3].data == b'data: request 3 server 0'
            assert results[4].data == b'data: request 4 server 1'
            assert results[5].data == b'data: request 5 server 2'
            assert results[6].data == b'data: request 6 server 0'


async def test_load_balancer_round_robin_request_stream(unused_tcp_port_factory):
    clients = []
    server_count = 3
    request_count = 7

    async with AsyncExitStack() as stack:
        for i in range(server_count):
            tcp_port = unused_tcp_port_factory()
            _, client = await stack.enter_async_context(
                pipe_factory_tcp(tcp_port,
                                 server_arguments={'handler_factory': IdentifiedHandlerFactory(i, Handler).factory},
                                 auto_connect_client=False))
            clients.append(client)

        round_robin = LoadBalancerRoundRobin(clients)
        async with LoadBalancerRSocket(round_robin) as load_balancer_client:
            awaitable_client = AwaitableRSocket(load_balancer_client)
            results = await asyncio.gather(
                *[awaitable_client.request_stream(Payload(('request %d' % j).encode()))
                  for j in range(request_count)]
            )

            assert results[0][0].data == b'data: request 0 server 0'
            assert results[1][0].data == b'data: request 1 server 1'
            assert results[2][0].data == b'data: request 2 server 2'
            assert results[3][0].data == b'data: request 3 server 0'
            assert results[4][0].data == b'data: request 4 server 1'
            assert results[5][0].data == b'data: request 5 server 2'
            assert results[6][0].data == b'data: request 6 server 0'


async def test_load_balancer_random_request_response(unused_tcp_port_factory):
    clients = []
    server_count = 3
    request_count = 70

    async with AsyncExitStack() as stack:
        for i in range(server_count):
            tcp_port = unused_tcp_port_factory()
            _, client = await stack.enter_async_context(
                pipe_factory_tcp(tcp_port,
                                 server_arguments={'handler_factory': IdentifiedHandlerFactory(i, Handler).factory},
                                 auto_connect_client=False))
            clients.append(client)

        strategy = LoadBalancerRandom(clients)
        async with LoadBalancerRSocket(strategy) as load_balancer_client:
            results = await asyncio.gather(
                *[load_balancer_client.request_response(Payload(('request %d' % j).encode()))
                  for j in range(request_count)]
            )

            server_ids = [payload.data.decode()[-1] for payload in results]
            assert '0' in server_ids
            assert '1' in server_ids
            assert '2' in server_ids
