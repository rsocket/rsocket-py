import asyncio
from contextlib import AsyncExitStack

from rsocket.load_balancer.load_balancer_rsocket import LoadBalancerRSocket
from rsocket.load_balancer.round_robin import LoadBalancerRoundRobin
from rsocket.payload import Payload
from tests.conftest import pipe_factory_tcp
from tests.rsocket.helpers import future_from_payload, IdentifiedHandlerFactory, IdentifiedHandler


class Handler(IdentifiedHandler):

    async def request_response(self, request: Payload):
        return future_from_payload(Payload(request.data + (' server %d' % self._server_id).encode(), request.metadata))


async def test_load_balancer_round_robin(unused_tcp_port_factory):
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
