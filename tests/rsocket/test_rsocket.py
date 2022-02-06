import asyncio
import logging
from datetime import timedelta

import pytest

from rsocket.payload import Payload
from rsocket.request_handler import BaseRequestHandler


async def test_rsocket_client_closed_without_requests(lazy_pipe):
    async with lazy_pipe():
        pass  # This should not raise an exception


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


async def test_rsocket_keepalive(pipe, caplog):
    caplog.set_level(logging.DEBUG)
    await asyncio.sleep(2)

    found_client_sent_keepalive = False
    found_client_received_keepalive = False
    found_server_sent_keepalive = False
    found_server_received_keepalive = False

    for record in caplog.records:
        if record.message == 'client: Sent keepalive':
            found_client_sent_keepalive = True
        if record.message == 'server: Received keepalive':
            found_server_received_keepalive = True
        if record.message == 'server: Responded to keepalive':
            found_server_sent_keepalive = True
        if record.message == 'client: Received keepalive':
            found_client_received_keepalive = True

        assert record.levelname not in ("CRITICAL", "ERROR", "WARNING")

    assert found_client_sent_keepalive
    assert found_client_received_keepalive
    assert found_server_sent_keepalive
    assert found_server_received_keepalive
