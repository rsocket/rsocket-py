import asyncio
from datetime import timedelta

import pytest

from rsocket.lease import SingleLeasePublisher
from rsocket.payload import Payload
from rsocket.request_handler import BaseRequestHandler


@pytest.mark.asyncio
async def test_rsocket_client_closed_without_requests(lazy_pipe):
    class Handler(BaseRequestHandler):
        async def request_response(self, request: Payload):
            future = asyncio.Future()
            future.set_result(Payload(b'data: ' + request.data,
                                      b'meta: ' + request.metadata))
            return future

    async with lazy_pipe(client_arguments={'honor_lease': True},
                         server_arguments={'handler_factory': Handler,
                                           'lease_publisher': SingleLeasePublisher(
                                               maximum_lease_time=timedelta(seconds=3)
                                           )}) as (server, client):
        pass
