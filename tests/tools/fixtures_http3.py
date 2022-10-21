from asyncio import Event
from contextlib import asynccontextmanager
from typing import Optional

from rsocket.helpers import single_transport_provider
from rsocket.rsocket_base import RSocketBase
from rsocket.rsocket_client import RSocketClient
from tests.rsocket.helpers import assert_no_open_streams
# noinspection PyUnresolvedReferences
from tests.tools.fixtures_shared import generate_test_certificates
from tests.tools.http3_client import http3_ws_transport
from tests.tools.http3_server import start_http_server


@asynccontextmanager
async def pipe_factory_http3(generate_test_certificates,
                             unused_tcp_port,
                             client_arguments=None,
                             server_arguments=None):
    certificate, private_key = generate_test_certificates

    server: Optional[RSocketBase] = None
    wait_for_server = Event()

    def store_server(new_server):
        nonlocal server
        server = new_server
        wait_for_server.set()

    http3_server = await start_http_server(host='localhost',
                                           port=unused_tcp_port,
                                           certificate=certificate,
                                           private_key=private_key,
                                           on_server_create=store_server,
                                           **(server_arguments or {}))

    # from datetime import timedelta
    # test_overrides = {'keep_alive_period': timedelta(minutes=20)}
    client_arguments = client_arguments or {}
    # client_arguments.update(test_overrides)
    async with http3_ws_transport(certificate, f'wss://localhost:{unused_tcp_port}/ws') as transport:
        async with RSocketClient(single_transport_provider(transport),
                                 **client_arguments) as client:
            await wait_for_server.wait()
            yield server, client

    await server.close()
    assert_no_open_streams(client, server)

    http3_server.close()
