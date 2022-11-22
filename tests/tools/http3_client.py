from contextlib import asynccontextmanager
from typing import cast
from urllib.parse import urlparse

from aioquic.asyncio.client import connect
from aioquic.h3.connection import H3_ALPN, ErrorCode

from rsocket.transports.http3_transport import Http3TransportWebsocket, RSocketHttp3ClientProtocol
from tests.tools.helpers import quic_client_configuration


@asynccontextmanager
async def http3_ws_transport(
        certificate,
        url: str
):
    parsed = urlparse(url)
    assert parsed.scheme in (
        "https",
        "wss",
    ), "Only https:// or wss:// URLs are supported."

    host = parsed.hostname

    if parsed.port is not None:
        port = parsed.port
    else:
        port = 443

    async with connect(
            host,
            port,
            configuration=quic_client_configuration(certificate, alpn_protocols=H3_ALPN),
            create_protocol=RSocketHttp3ClientProtocol
    ) as client:
        client = cast(RSocketHttp3ClientProtocol, client)

        if parsed.scheme == "wss":
            ws = await client.websocket(url)
            transport = Http3TransportWebsocket(ws)
            yield transport
            await ws.close()

        client._quic.close(error_code=ErrorCode.H3_NO_ERROR)
