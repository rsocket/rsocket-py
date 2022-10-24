import asyncio
import logging
from dataclasses import dataclass
from typing import Optional, Type, Collection

import asyncclick as click

from rsocket.awaitable.awaitable_rsocket import AwaitableRSocket
from rsocket.extensions.helpers import route, composite, authenticate_simple, authenticate_bearer
from rsocket.extensions.mimetypes import WellKnownMimeTypes
from rsocket.frame import MAX_REQUEST_N
from rsocket.frame_helpers import ensure_bytes
from rsocket.helpers import single_transport_provider
from rsocket.payload import Payload
from rsocket.rsocket_client import RSocketClient
from rsocket.transports.abstract_messaging import AbstractMessagingTransport
from rsocket.transports.aiohttp_websocket import TransportAioHttpClient
from rsocket.transports.tcp import TransportTCP
from importlib.metadata import version as get_version


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


def build_composite_metadata(auth_simple: str,
                             route_value: str,
                             auth_bearer: str):
    composite_items = []

    if route_value is not None:
        composite_items.append(route(route_value))

    if auth_simple is not None:
        composite_items.append(authenticate_simple(*auth_simple.split(':')))

    if auth_bearer is not None:
        composite_items.append(authenticate_bearer(auth_bearer))

    return composite_items


@click.command()
@click.option('-d', '--data', is_flag=False, help='Data. Use "-" to read data from standard input. (default: )')
@click.option('-l', '--load', is_flag=False, help='Load a file as Data. (e.g. ./foo.txt, /tmp/foo.txt)')
@click.option('-m', '--metadata', is_flag=False, default=None, help='Metadata (default: )')
@click.option('-r', '--route', 'route_value', is_flag=False, default=None, help='Enable Routing Metadata Extension')
@click.option('--limitRate', 'limit_rate', is_flag=False, default=None, type=int, help='Enable limitRate(rate)')
@click.option('--take', 'take_n', is_flag=False, default=None, type=int)
@click.option('-u', '--as', '--authSimple', 'auth_simple', is_flag=False, default=None,
              help='Enable Authentication Metadata Extension (Simple). The format must be "username: password"')
@click.option('--sd', '--setupData', 'setup_data', is_flag=False, default=None)
@click.option('--sm', '--setupMetadata', 'setup_metadata', is_flag=False, default=None)
@click.option('--ab', '--authBearer', 'auth_bearer', is_flag=False, default=None,
              help='Enable Authentication Metadata Extension (Bearer)')
@click.option('--dataMimeType', '--dmt', 'data_mime_type', is_flag=False,
              help='MimeType for data (default: application/json)')
@click.option('--metadataMimeType', '--mmt', 'metadata_mime_type', is_flag=False,
              help='MimeType for metadata (default:application/json)')
@click.option('--request', is_flag=True)
@click.option('--stream', is_flag=True)
@click.option('--channel', is_flag=True)
@click.option('--fnf', is_flag=True)
@click.option('--debug', is_flag=True, help='Show debug log')
@click.option('--quiet', '-q', is_flag=True, help='Disable the output on next')
@click.option('--version', is_flag=True, help='Print version')
@click.argument('uri')
async def command(data, load,
                  metadata, route_value, auth_simple, auth_bearer,
                  limit_rate, take_n,
                  setup_data, setup_metadata,
                  data_mime_type, metadata_mime_type,
                  request, stream, channel, fnf,
                  uri, debug, version, quiet):
    if version:
        try:
            print(get_version('rsocket'))
        except Exception:
            print('Failed to find version')
        return

    if quiet:
        logging.basicConfig(handlers=[])

    if debug:
        logging.basicConfig(level=logging.DEBUG)

    if take_n == 0:
        return

    limit_rate = normalize_limit_rate(limit_rate)

    parsed_uri = parse_uri(uri)

    composite_items = build_composite_metadata(auth_simple, route_value, auth_bearer)

    data = normalize_data(data, load)

    transport = await transport_from_uri(parsed_uri)

    if len(composite_items) > 0:
        metadata_mime_type = WellKnownMimeTypes.MESSAGE_RSOCKET_COMPOSITE_METADATA

    setup_payload = create_setup_payload(setup_data, setup_metadata)

    async with RSocketClient(single_transport_provider(transport),
                             data_encoding=data_mime_type or WellKnownMimeTypes.APPLICATION_JSON,
                             metadata_encoding=metadata_mime_type or WellKnownMimeTypes.APPLICATION_JSON,
                             setup_payload=setup_payload) as client:
        awaitable_client = AwaitableRSocket(client)

        metadata_value = get_metadata_value(composite_items, metadata)

        payload = Payload(ensure_bytes(data), metadata_value)

        result = await execute_request(awaitable_client,
                                       channel,
                                       fnf,
                                       limit_rate,
                                       payload,
                                       request,
                                       stream)

        if not quiet:
            output_result(result)


def output_result(result):
    if isinstance(result, Payload):
        print(result.data.decode('utf-8'))
    elif isinstance(result, Collection):
        print([p.data.decode('utf-8') for p in result])


async def execute_request(awaitable_client, channel, fnf, limit_rate, payload, request, stream):
    result = None

    if request:
        result = await awaitable_client.request_response(payload)
    elif stream:
        result = await awaitable_client.request_stream(payload, limit_rate=limit_rate)
    elif channel:
        result = await awaitable_client.request_channel(payload, limit_rate=limit_rate)
    elif fnf:
        await awaitable_client.fire_and_forget(payload)

    return result


def get_metadata_value(composite_items, metadata) -> bytes:
    if len(composite_items) > 0:
        metadata_value = composite(*composite_items)
    else:
        metadata_value = metadata

    return ensure_bytes(metadata_value)


def create_setup_payload(setup_data, setup_metadata) -> Optional[Payload]:
    setup_payload = None
    if setup_data is not None or setup_metadata is not None:
        setup_payload = Payload(
            ensure_bytes(setup_data),
            ensure_bytes(setup_metadata)
        )
    return setup_payload


def normalize_data(data: str, load: str) -> bytes:
    if data == '-':
        stdin_text = click.get_text_stream('stdin')
        data = stdin_text.read()

    if load:
        with open(load) as fd:
            data = fd.read()

    return ensure_bytes(data)


def normalize_limit_rate(limit_rate):
    if limit_rate is not None and not limit_rate > 0:
        limit_rate = MAX_REQUEST_N
    else:
        limit_rate = MAX_REQUEST_N
    return limit_rate


if __name__ == '__main__':
    command()
