import asyncio
import logging
from contextlib import asynccontextmanager
from dataclasses import dataclass
from typing import Optional, Type, Collection, List

import aiohttp
import asyncclick as click
from werkzeug.routing import Map

from rsocket.awaitable.awaitable_rsocket import AwaitableRSocket
from rsocket.extensions.helpers import route, composite, authenticate_simple, authenticate_bearer
from rsocket.extensions.mimetypes import WellKnownMimeTypes
from rsocket.frame import MAX_REQUEST_N
from rsocket.frame_helpers import ensure_bytes, safe_len
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


def parse_uri(uri: str) -> RSocketUri:
    schema, rest = uri.split(':', 1)
    rest = rest.strip('/')
    host_port = rest.split('/', 1)
    host, port = host_port[0].split(':')

    if len(host_port) > 1:
        rest = host_port[1]
    else:
        rest = None

    return RSocketUri(host, port, schema, rest, uri)


@asynccontextmanager
async def transport_from_uri(uri: RSocketUri,
                             verify_ssl=True,
                             headers: Optional[Map] = None) -> Type[AbstractMessagingTransport]:
    if uri.schema == 'tcp':
        connection = await asyncio.open_connection(uri.host, uri.port)
        yield TransportTCP(*connection)
    elif uri.schema in ['wss', 'ws']:
        async with aiohttp.ClientSession() as session:
            async with session.ws_connect(uri.original_uri,
                                          verify_ssl=verify_ssl,
                                          headers=headers) as websocket:
                yield TransportAioHttpClient(websocket=websocket)
    else:
        raise Exception('Unsupported schema in CLI')


def build_composite_metadata(auth_simple: Optional[str],
                             route_value: Optional[str],
                             auth_bearer: Optional[str]):
    composite_items = []

    if route_value is not None:
        composite_items.append(route(route_value))

    if auth_simple is not None:
        composite_items.append(authenticate_simple(*auth_simple.split(':')))

    if auth_bearer is not None:
        composite_items.append(authenticate_bearer(auth_bearer))

    return composite_items


@asynccontextmanager
async def create_client(parsed_uri,
                        data_mime_type,
                        metadata_mime_type,
                        setup_payload,
                        allow_untrusted_ssl=False,
                        http_headers=None):
    async with transport_from_uri(parsed_uri, verify_ssl=not allow_untrusted_ssl, headers=http_headers) as transport:
        async with RSocketClient(single_transport_provider(transport),
                                 data_encoding=data_mime_type or WellKnownMimeTypes.APPLICATION_JSON,
                                 metadata_encoding=metadata_mime_type or WellKnownMimeTypes.APPLICATION_JSON,
                                 setup_payload=setup_payload) as client:
            yield AwaitableRSocket(client)


@click.command(name='rsocket-py', help='Supported connection strings: tcp/ws/wss')
@click.option('--request', is_flag=True,
              help='Request response')
@click.option('--stream', is_flag=True,
              help='Request stream')
@click.option('--channel', is_flag=True,
              help='Request channel')
@click.option('--fnf', is_flag=True,
              help='Fire and Forget')
@click.option('-d', '--data', is_flag=False,
              help='Data. Use "-" to read data from standard input. (default: )')
@click.option('-l', '--load', is_flag=False,
              help='Load a file as Data. (e.g. ./foo.txt, /tmp/foo.txt)')
@click.option('-m', '--metadata', is_flag=False, default=None,
              help='Metadata (default: )')
@click.option('-r', '--route', 'route_value', is_flag=False, default=None,
              help='Enable Routing Metadata Extension')
@click.option('--limitRate', 'limit_rate', is_flag=False, default=None, type=int,
              help='Enable limitRate(rate)')
@click.option('--take', 'take_n', is_flag=False, default=None, type=int,
              help='Enable take(n)')
@click.option('-u', '--as', '--authSimple', 'auth_simple', is_flag=False, default=None,
              help='Enable Authentication Metadata Extension (Simple). The format must be "username: password"')
@click.option('--sd', '--setupData', 'setup_data', is_flag=False, default=None,
              help='Data for Setup payload')
@click.option('--sm', '--setupMetadata', 'setup_metadata', is_flag=False, default=None,
              help='Metadata for Setup payload')
@click.option('--ab', '--authBearer', 'auth_bearer', is_flag=False, default=None,
              help='Enable Authentication Metadata Extension (Bearer)')
@click.option('--dataMimeType', '--dmt', 'data_mime_type', is_flag=False,
              help='MimeType for data (default: application/json)')
@click.option('--metadataMimeType', '--mmt', 'metadata_mime_type', is_flag=False,
              help='MimeType for metadata (default:application/json)')
@click.option('--allowUntrustedSsl', 'allow_untrusted_ssl', is_flag=True, default=False,
              help='Do not verify SSL certificate (for wss:// urls)')
@click.option('--httpHeader', 'http_header', multiple=True,
              help='ws/wss headers')
@click.option('--debug', is_flag=True,
              help='Show debug log')
@click.option('--quiet', '-q', is_flag=True,
              help='Disable the output on next')
@click.option('--version', is_flag=True,
              help='Print version')
@click.argument('uri')
async def command(data, load,
                  metadata, route_value, auth_simple, auth_bearer,
                  limit_rate, take_n, allow_untrusted_ssl,
                  setup_data, setup_metadata,
                  http_header,
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

    http_headers = parse_headers(http_header)

    composite_items = build_composite_metadata(auth_simple, route_value, auth_bearer)

    async with create_client(parse_uri(uri),
                             data_mime_type,
                             normalize_metadata_mime_type(composite_items, metadata_mime_type),
                             create_setup_payload(setup_data, setup_metadata),
                             allow_untrusted_ssl=allow_untrusted_ssl,
                             http_headers=http_headers
                             ) as client:

        result = await execute_request(client,
                                       channel,
                                       fnf,
                                       normalize_limit_rate(limit_rate),
                                       create_request_payload(data, load, metadata, composite_items),
                                       request,
                                       stream)

        if not quiet:
            output_result(result)


def parse_headers(http_headers):
    if safe_len(http_headers) > 0:
        headers = dict()

        for header in http_headers:
            parts = header.split('=', 2)
            headers[parts[0]] = parts[1]

        return headers

    return None


def normalize_metadata_mime_type(composite_items, metadata_mime_type):
    if len(composite_items) > 0:
        metadata_mime_type = WellKnownMimeTypes.MESSAGE_RSOCKET_COMPOSITE_METADATA

    return metadata_mime_type


def create_request_payload(data: Optional[str],
                           load: Optional[str],
                           metadata: Optional[str],
                           composite_items: List) -> Payload:
    data = normalize_data(data, load)
    metadata_value = get_metadata_value(composite_items, metadata)
    return Payload(data, metadata_value)


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


def get_metadata_value(composite_items: List, metadata: Optional[str]) -> bytes:
    if len(composite_items) > 0:
        metadata_value = composite(*composite_items)
    else:
        metadata_value = metadata

    return ensure_bytes(metadata_value)


def create_setup_payload(setup_data: Optional[str], setup_metadata: Optional[str]) -> Optional[Payload]:
    setup_payload = None

    if setup_data is not None or setup_metadata is not None:
        setup_payload = Payload(
            ensure_bytes(setup_data),
            ensure_bytes(setup_metadata)
        )

    return setup_payload


def normalize_data(data: Optional[str], load: Optional[str]) -> bytes:
    if data == '-':
        stdin_text = click.get_text_stream('stdin')
        return ensure_bytes(stdin_text.read())

    if load is not None:
        with open(load) as fd:
            return ensure_bytes(fd.read())

    return ensure_bytes(data)


def normalize_limit_rate(limit_rate):
    if limit_rate is not None and not limit_rate > 0:
        limit_rate = MAX_REQUEST_N
    else:
        limit_rate = MAX_REQUEST_N

    return limit_rate


if __name__ == '__main__':
    command()
