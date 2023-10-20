from asyncio import Event
from contextlib import asynccontextmanager
from typing import Optional

from rsocket.helpers import single_transport_provider
from rsocket.rsocket_base import RSocketBase
from rsocket.rsocket_client import RSocketClient
from tests.rsocket.helpers import assert_no_open_streams


@asynccontextmanager
async def pipe_factory_quic(generate_test_certificates,
                            unused_tcp_port,
                            client_arguments=None,
                            server_arguments=None):
    from rsocket.transports.aioquic_transport import rsocket_connect, rsocket_serve
    from tests.tools.helpers import quic_client_configuration
    from aioquic.quic.configuration import QuicConfiguration

    certificate, private_key = generate_test_certificates

    server_configuration = QuicConfiguration(
        certificate=certificate,
        private_key=private_key,
        is_client=False
    )

    server: Optional[RSocketBase] = None
    wait_for_server = Event()

    def store_server(new_server):
        nonlocal server
        server = new_server
        wait_for_server.set()

    quic_server = await rsocket_serve(host='localhost',
                                      port=unused_tcp_port,
                                      configuration=server_configuration,
                                      on_server_create=store_server,
                                      **(server_arguments or {}))

    try:
        # from datetime import timedelta
        # test_overrides = {'keep_alive_period': timedelta(minutes=20)}
        client_arguments = client_arguments or {}
        # client_arguments.update(test_overrides)
        async with rsocket_connect('localhost', unused_tcp_port,
                                   configuration=quic_client_configuration(certificate)) as transport:
            async with RSocketClient(single_transport_provider(transport),
                                     **client_arguments) as client:
                await wait_for_server.wait()
                yield server, client
    finally:
        if server is not None:
            await server.close()

        assert_no_open_streams(client, server)
        quic_server.close()
