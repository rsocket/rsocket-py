import asyncio
from dataclasses import dataclass
from typing import Optional, Type

import asyncclick as click

from rsocket.awaitable.awaitable_rsocket import AwaitableRSocket
from rsocket.extensions.helpers import route, composite, authenticate_simple
from rsocket.extensions.mimetypes import WellKnownMimeTypes
from rsocket.frame_helpers import ensure_bytes
from rsocket.helpers import single_transport_provider
from rsocket.payload import Payload
from rsocket.rsocket_client import RSocketClient
from rsocket.transports.abstract_messaging import AbstractMessagingTransport
from rsocket.transports.aiohttp_websocket import TransportAioHttpClient
from rsocket.transports.tcp import TransportTCP


@dataclass(frozen=True)
class RSocketUri:
    host: str
    port: str
    schema: str
    path: Optional[str] = None
    original_uri: Optional[str] = None


def parse_uri(uri: str):
    schema, rest = uri.split(':', 1)
    rest = rest.strip('/')
    host_port = rest.split('/', 1)
    host, port = host_port[0].split(':')

    if len(host_port) > 1:
        rest = host_port[1]
    else:
        rest = None

    return RSocketUri(host, port, schema, rest, uri)


async def transport_from_uri(uri: RSocketUri) -> Type[AbstractMessagingTransport]:
    if uri.schema == 'tcp':
        connection = await asyncio.open_connection(uri.host, uri.port)
        return TransportTCP(*connection)
    elif uri.schema == 'ws':
        return TransportAioHttpClient(uri.original_uri)

    raise Exception('Unsupported schema in CLI')


def build_composite_metadata(auth_simple, route_value):
    composite_items = []
    if route_value is not None:
        composite_items.append(route(route_value))
    if auth_simple is not None:
        composite_items.append(authenticate_simple(*auth_simple.split(':')))
    return composite_items


@click.command()
@click.option('-d', '--data', is_flag=False)
@click.option('-m', '--metadata', is_flag=False, default=None)
@click.option('-r', '--route', 'route_value', is_flag=False, default=None)
@click.option('-u', '--as', '--authSimple', 'auth_simple', is_flag=False, default=None)
@click.option('--dataMimeType', is_flag=False, default='application/json')
@click.option('--request', is_flag=True)
@click.option('--stream', is_flag=True)
@click.option('--channel', is_flag=True)
@click.option('--fnf', is_flag=True)
@click.option('--version', is_flag=True)
@click.argument('uri')
async def command(data,
                  metadata, route_value, auth_simple,
                  datamimetype,
                  request, stream, channel, fnf,
                  uri, version):
    parsed_uri = parse_uri(uri)

    composite_items = build_composite_metadata(auth_simple, route_value)

    transport = await transport_from_uri(parsed_uri)

    client_arguments = {}

    if len(composite_items) > 0:
        client_arguments['metadata_encoding'] = WellKnownMimeTypes.MESSAGE_RSOCKET_COMPOSITE_METADATA

    async with RSocketClient(single_transport_provider(transport), **client_arguments) as client:
        awaitable_client = AwaitableRSocket(client)

        if len(composite_items) > 0:
            metadata_value = composite(*composite_items)
        else:
            metadata_value = metadata

        payload = Payload(ensure_bytes(data), metadata_value)

        result = None
        if request:
            result = await awaitable_client.request_response(payload)
        elif stream:
            result = await awaitable_client.request_stream(payload)
        elif channel:
            result = await awaitable_client.request_channel(payload)
        elif fnf:
            await awaitable_client.fire_and_forget(payload)

        if result is not None:
            print(result.data.decode('utf-8'))


if __name__ == '__main__':
    command()
