import asyncio
import logging
from datetime import timedelta
from typing import Callable

import pytest

from rsocket.error_codes import ErrorCode
from rsocket.exceptions import RSocketProtocolException
from rsocket.payload import Payload
from rsocket.request_handler import BaseRequestHandler
from tests.rsocket.helpers import create_future


async def test_rsocket_client_closed_without_requests(lazy_pipe):
    async with lazy_pipe():
        pass  # This should not raise an exception


async def test_rsocket_max_server_keepalive_reached_and_request_not_canceled_by_default(lazy_pipe_tcp):
    """todo: find why test only works using tcp transport"""
    class Handler(BaseRequestHandler):
        async def request_response(self, request: Payload):
            await asyncio.sleep(4)
            return create_future(Payload(b'response'))

    async with lazy_pipe_tcp(
            client_arguments={
                'keep_alive_period': timedelta(seconds=2),
                'max_lifetime_period': timedelta(seconds=1)
            },
            server_arguments={'handler_factory': Handler}) as (server, client):
        result = await client.request_response(Payload(b'dog', b'cat'))

        assert result.data == b'response'


async def test_rsocket_max_server_keepalive_reached_and_request_canceled_explicitly(lazy_pipe):
    class Handler(BaseRequestHandler):
        async def request_response(self, request: Payload):
            await asyncio.sleep(4)
            return create_future(Payload(b'response'))

    class ClientHandler(BaseRequestHandler):

        async def on_keepalive_timeout(self,
                                       time_since_last_keepalive: timedelta,
                                       cancel_all_streams: Callable):
            cancel_all_streams()

    async with lazy_pipe(
            client_arguments={
                'keep_alive_period': timedelta(seconds=2),
                'max_lifetime_period': timedelta(seconds=1),
                'handler_factory': ClientHandler},
            server_arguments={'handler_factory': Handler}) as (server, client):
        with pytest.raises(RSocketProtocolException) as exc_info:
            await client.request_response(Payload(b'dog', b'cat'))

        assert exc_info.value.data == 'Server not alive'
        assert exc_info.value.error_code == ErrorCode.CANCELED


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
