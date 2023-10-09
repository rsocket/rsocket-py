import logging

import aiohttp
import asyncclick as click

from rsocket.helpers import single_transport_provider
from rsocket.payload import Payload
from rsocket.rsocket_client import RSocketClient
from rsocket.transports.aiohttp_websocket import TransportAioHttpClient
from rsocket.transports.aiohttp_websocket import websocket_client


async def application(with_ssl: bool, serve_port: int):
    if with_ssl:
        async with aiohttp.ClientSession() as session:
            async with session.ws_connect('wss://localhost:%s' % serve_port, verify_ssl=False) as websocket:
                async with RSocketClient(
                        single_transport_provider(TransportAioHttpClient(websocket=websocket))) as client:
                    result = await client.request_response(Payload(b'ping'))
                    print(result)

    else:
        async with websocket_client('http://localhost:%s' % serve_port) as client:
            result = await client.request_response(Payload(b'ping'))
            print(result.data)


@click.command()
@click.option('--with-ssl', is_flag=False, default=False)
@click.option('--port', is_flag=False, default=6565)
async def command(with_ssl, port: int):
    logging.basicConfig(level=logging.DEBUG)
    await application(with_ssl, port)


if __name__ == '__main__':
    command()
