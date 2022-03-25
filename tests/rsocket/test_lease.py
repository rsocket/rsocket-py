import asyncio
from datetime import timedelta

import pytest

from reactivestreams.subscriber import Subscriber
from rsocket.exceptions import RSocketProtocolError
from rsocket.lease import SingleLeasePublisher, DefinedLease
from rsocket.payload import Payload
from rsocket.request_handler import BaseRequestHandler
from tests.rsocket.helpers import future_from_payload


class PeriodicalLeasePublisher(SingleLeasePublisher):

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self._lease_task = None

    def subscribe(self, subscriber: Subscriber):
        self._lease_task = asyncio.create_task(self._subscribe_loop(subscriber))

    async def _subscribe_loop(self, subscriber: Subscriber):
        try:
            while True:
                await asyncio.sleep(self.wait_between_leases.total_seconds())

                subscriber.on_next(DefinedLease(
                    maximum_request_count=self.maximum_request_count,
                    maximum_lease_time=self.maximum_lease_time
                ))
        except asyncio.CancelledError:
            pass

    async def __aenter__(self):
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        if self._lease_task is not None:
            self._lease_task.cancel()
            await self._lease_task


async def test_request_response_with_server_side_lease_works(lazy_pipe):
    class Handler(BaseRequestHandler):
        async def request_response(self, request: Payload):
            return future_from_payload(request)

    async with lazy_pipe(client_arguments={'honor_lease': True},
                         server_arguments={'handler_factory': Handler,
                                           'lease_publisher': SingleLeasePublisher(
                                               maximum_lease_time=timedelta(seconds=3)
                                           )}) as (server, client):
        for x in range(2):
            response = await client.request_response(Payload(b'dog', b'cat'))
            assert response == Payload(b'data: dog', b'meta: cat')


@pytest.mark.timeout(15)
async def test_request_response_with_client_and_server_side_lease_works(lazy_pipe):
    class Handler(BaseRequestHandler):
        async def request_response(self, request: Payload):
            return future_from_payload(request)

    async with PeriodicalLeasePublisher(
            maximum_request_count=2,
            maximum_lease_time=timedelta(seconds=3),
            wait_between_leases=timedelta(seconds=2)) as client_leases:
        async with PeriodicalLeasePublisher(
                maximum_request_count=2,
                maximum_lease_time=timedelta(seconds=3),
                wait_between_leases=timedelta(seconds=2)) as server_leases:
            async with lazy_pipe(
                    client_arguments={'honor_lease': True,
                                      'handler_factory': Handler,
                                      'lease_publisher': client_leases},
                    server_arguments={'honor_lease': True,
                                      'handler_factory': Handler,
                                      'lease_publisher': server_leases}) as (server, client):
                for x in range(3):
                    response = await client.request_response(Payload(b'dog', b'cat'))
                    assert response == Payload(b'data: dog', b'meta: cat')

                for x in range(3):
                    response = await server.request_response(Payload(b'dog', b'cat'))
                    assert response == Payload(b'data: dog', b'meta: cat')


async def test_request_response_with_lease_too_many_requests(lazy_pipe):
    class Handler(BaseRequestHandler):
        async def request_response(self, request: Payload):
            return future_from_payload(request)

    async with lazy_pipe(client_arguments={'honor_lease': True},
                         server_arguments={'handler_factory': Handler,
                                           'lease_publisher': SingleLeasePublisher(
                                               maximum_request_count=2
                                           )}) as (server, client):
        for x in range(2):
            response = await client.request_response(Payload(b'dog', b'cat'))
            assert response == Payload(b'data: dog', b'meta: cat')

        with pytest.raises(asyncio.TimeoutError):
            await asyncio.wait_for(client.request_response(Payload(b'invalid request')), 3)


@pytest.mark.timeout(15)
async def test_request_response_with_lease_client_side_exception_requests_late(lazy_pipe):
    class Handler(BaseRequestHandler):
        async def request_response(self, request: Payload):
            return future_from_payload(request)

    async with lazy_pipe(client_arguments={'honor_lease': True},
                         server_arguments={'handler_factory': Handler,
                                           'lease_publisher': SingleLeasePublisher(
                                               maximum_lease_time=timedelta(seconds=3)
                                           )}) as (server, client):
        for x in range(2):
            response = await client.request_response(Payload(b'dog', b'cat'))
            assert response == Payload(b'data: dog', b'meta: cat')

        await asyncio.sleep(5)

        with pytest.raises(asyncio.TimeoutError):
            await asyncio.wait_for(client.request_response(Payload(b'invalid request')), 3)


@pytest.mark.allow_error_log(regex_filter='UNSUPPORTED_SETUP')
async def test_server_rejects_all_requests_if_lease_not_supported(lazy_pipe):
    async with lazy_pipe(client_arguments={'honor_lease': True}) as (server, client):
        with pytest.raises(asyncio.TimeoutError):
            await asyncio.wait_for(client.request_response(Payload(b'invalid request')), 3)


@pytest.mark.skip(reason='TODO')
async def test_request_response_with_lease_server_side_exception(lazy_pipe):
    class Handler(BaseRequestHandler):
        async def request_response(self, request: Payload):
            return future_from_payload(request)

    async with lazy_pipe(client_arguments={'honor_lease': True},
                         server_arguments={'handler_factory': Handler,
                                           'lease_publisher': SingleLeasePublisher(
                                               maximum_request_count=2
                                           )}) as (server, client):
        for x in range(2):
            response = await client.request_response(Payload(b'dog', b'cat'))
            assert response == Payload(b'data: dog', b'meta: cat')

        with pytest.raises(RSocketProtocolError):
            await client.request_response(Payload(b'invalid request'))
