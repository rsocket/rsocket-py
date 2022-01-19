import asyncio
from datetime import timedelta

import pytest

from rsocket.payload import Payload
from rsocket.request_handler import BaseRequestHandler


@pytest.mark.asyncio
async def test_rsocket_client_closed_without_requests(lazy_pipe):
    async with lazy_pipe():
        pass  # This should not raise an exception


@pytest.mark.asyncio
async def test_rsocket_max_server_keepalive_reached(lazy_pipe):
    class Handler(BaseRequestHandler):
        async def request_response(self, request: Payload):
            await asyncio.sleep(5)

    async with lazy_pipe(
            client_arguments={
                'keep_alive_period': timedelta(seconds=10),
                'max_lifetime_period': timedelta(seconds=1)},
            server_arguments={'handler_factory': Handler}) as (server, client):
        with pytest.raises(Exception):
            await client.request_response(Payload(b'dog', b'cat'))
