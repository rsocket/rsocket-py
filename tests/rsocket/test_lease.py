import asyncio
from datetime import timedelta

import pytest

from rsocket.exceptions import RSocketRejected, RSocketLeaseNotReceivedTimeoutError
from rsocket.lease import SingleLeasePublisher
from rsocket.payload import Payload
from rsocket.request_handler import BaseRequestHandler


@pytest.mark.asyncio
async def test_request_response_with_lease_client_side_exception_too_many_requests(lazy_pipe):
    class Handler(BaseRequestHandler):
        async def request_response(self, request: Payload):
            future = asyncio.Future()
            future.set_result(Payload(b'data: ' + request.data,
                                      b'meta: ' + request.metadata))
            return future

    async with lazy_pipe(client_arguments={'honor_lease': True},
                         server_arguments={'handler_factory': Handler,
                                           'lease_publisher': SingleLeasePublisher(
                                               maximum_request_count=2
                                           )}) as (server, client):
        for x in range(2):
            response = await client.request_response(Payload(b'dog', b'cat'))
            assert response == Payload(b'data: dog', b'meta: cat')

        with pytest.raises(RSocketRejected):
            await client.request_response(Payload(b'invalid request'))


@pytest.mark.asyncio
async def test_request_response_with_lease_client_side_exception_requests_late(lazy_pipe):
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
        for x in range(2):
            response = await client.request_response(Payload(b'dog', b'cat'))
            assert response == Payload(b'data: dog', b'meta: cat')

        await asyncio.sleep(5)

        with pytest.raises(RSocketRejected):
            await client.request_response(Payload(b'invalid request'))


@pytest.mark.asyncio
async def test_server_rejects_connection_if_no_lease_supported(lazy_pipe):
    with pytest.raises(RSocketRejected):
        async with lazy_pipe(client_arguments={'honor_lease': True}) as (server, client):
            pass


@pytest.mark.asyncio
async def test_server_rejects_connection_lease_response_timeout(lazy_pipe):
    with pytest.raises(RSocketLeaseNotReceivedTimeoutError):
        async with lazy_pipe(client_arguments={'honor_lease': True,
                                               'lease_receive_timeout': timedelta(seconds=2)},
                             server_arguments={'lease_publisher': SingleLeasePublisher(
                                 sleep_time=timedelta(seconds=3)
                             )}) as (server, client):
            pass


@pytest.mark.asyncio
async def test_request_response_with_lease_server_side_exception(lazy_pipe):
    class Handler(BaseRequestHandler):
        async def request_response(self, request: Payload):
            future = asyncio.Future()
            future.set_result(Payload(b'data: ' + request.data,
                                      b'meta: ' + request.metadata))
            return future

    async with lazy_pipe(client_arguments={'honor_lease': True},
                         server_arguments={'handler_factory': Handler,
                                           'lease_publisher': SingleLeasePublisher(
                                               maximum_request_count=2
                                           )}) as (server, client):
        for x in range(2):
            response = await client._request_response(Payload(b'dog', b'cat'))
            assert response == Payload(b'data: dog', b'meta: cat')

        with pytest.raises(RSocketRejected):
            await client._request_response(Payload(b'invalid request'))
