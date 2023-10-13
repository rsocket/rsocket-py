import asyncio
import logging
import ssl
from contextlib import asynccontextmanager
from dataclasses import dataclass
from enum import Enum, unique
from importlib.metadata import version as get_version
from pathlib import Path
from typing import Optional, Type, Collection, List, Callable

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


@unique
class RequestType(Enum):
    response = 'REQUEST_RESPONSE'
    stream = 'REQUEST_STREAM'
    channel = 'REQUEST_CHANNEL'
    fnf = 'FIRE_AND_FORGET'
    metadata_push = 'METADATA_PUSH'


interaction_models: List[str] = [str(e.value) for e in RequestType]


@dataclass(frozen=True)
class RSocketUri:
    host: str
    port: int
    schema: str
    path: Optional[str] = None
    original_uri: Optional[str] = None


def parse_uri(uri: str) -> RSocketUri:
    schema, rest = uri.split(':', 1)
    rest = rest.strip('/')
    host_port_path = rest.split('/', 1)
    host_port = host_port_path[0].split(':')
    host = host_port[0]

    if len(host_port) == 1:
        port = None
    else:
        port = int(host_port[1])

    if len(host_port_path) > 1:
        rest = host_port_path[1]
    else:
        rest = None

    return RSocketUri(host, port, schema, rest, uri)


@asynccontextmanager
async def transport_from_uri(uri: RSocketUri,
                             verify_ssl=True,
                             headers: Optional[Map] = None,
                             trust_cert: Optional[str] = None) -> Type[AbstractMessagingTransport]:
    if uri.schema == 'tcp':
        connection = await asyncio.open_connection(uri.host, uri.port)
        yield TransportTCP(*connection)
    elif uri.schema in ['wss', 'ws']:
        async with aiohttp.ClientSession() as session:
            if trust_cert is not None:
                ssl_context = ssl.create_default_context(cafile=trust_cert)
            else:
                ssl_context = None

            async with session.ws_connect(uri.original_uri,
                                          verify_ssl=verify_ssl,
                                          ssl_context=ssl_context,
                                          headers=headers) as websocket:
                yield TransportAioHttpClient(websocket=websocket)
    else:
        raise Exception('Unsupported schema in CLI')


def build_composite_metadata(auth_simple: Optional[str],
                             route_value: Optional[str],
                             auth_bearer: Optional[str]) -> List:
    composite_items = []

    if route_value is not None:
        composite_items.append(route(route_value))

    if auth_simple is not None and auth_bearer is not None:
        raise click.UsageError('Multiple authentication methods specified.')

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
                        http_headers=None,
                        trust_cert=None):
    async with transport_from_uri(parsed_uri,
                                  verify_ssl=not allow_untrusted_ssl,
                                  headers=http_headers,
                                  trust_cert=trust_cert) as transport:
        async with RSocketClient(single_transport_provider(transport),
                                 data_encoding=data_mime_type or WellKnownMimeTypes.APPLICATION_JSON,
                                 metadata_encoding=metadata_mime_type or WellKnownMimeTypes.APPLICATION_JSON,
                                 setup_payload=setup_payload) as client:
            yield AwaitableRSocket(client)


@asynccontextmanager
async def create_gql_client(parsed_uri,
                            schema_file: Path,
                            allow_untrusted_ssl=False,
                            http_headers=None,
                            trust_cert=None):
    async with transport_from_uri(parsed_uri,
                                  verify_ssl=not allow_untrusted_ssl,
                                  headers=http_headers,
                                  trust_cert=trust_cert) as transport:
        async with RSocketClient(single_transport_provider(transport),
                                 metadata_encoding=WellKnownMimeTypes.MESSAGE_RSOCKET_COMPOSITE_METADATA) as client:
            with schema_file.open() as fd:
                schema = fd.read()

            from gql import Client
            from rsocket.graphql.rsocket_transport import RSocketTransport
            with Client(
                    schema=schema,
                    transport=RSocketTransport(client),
            ) as graphql:
                yield graphql


def get_request_type(request: bool,
                     stream: bool,
                     fnf: bool,
                     metadata_push: bool,
                     channel: bool,
                     interaction_model: Optional[str]) -> RequestType:
    interaction_options = list(filter(lambda _: _ is True, [request, stream, fnf, channel, metadata_push]))

    if len(interaction_options) >= 2 or (len(interaction_options) >= 1 and interaction_model is not None):
        raise click.UsageError('Multiple interaction methods specified.')

    if interaction_model is not None:
        return RequestType(interaction_model.upper())
    if request:
        return RequestType.response
    if stream:
        return RequestType.stream
    if channel:
        return RequestType.channel
    if fnf:
        return RequestType.fnf
    if metadata_push:
        return RequestType.metadata_push

    raise click.UsageError('No interaction method specified (eg. --request)')


@click.command(name='rsocket-py', help='Supported connection strings: tcp/ws/wss')
@click.option('--im', '--interactionModel', 'interaction_model', is_flag=False,
              type=click.Choice(interaction_models, case_sensitive=False),
              help='Interaction Model')
@click.option('--request', is_flag=True,
              help='Request response')
@click.option('--stream', is_flag=True,
              help='Request stream')
@click.option('--channel', is_flag=True,
              help='Request channel')
@click.option('--fnf', is_flag=True,
              help='Fire and Forget')
@click.option('--metadataPush', 'metadata_push', is_flag=True,
              help='Metadata Push')
@click.option('-d', '--data', '--input', 'data', is_flag=False,
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
@click.option('--sd', '--setup', '--setupData', 'setup_data', is_flag=False, default=None,
              help='Data for Setup payload')
@click.option('--sm', '--setupMetadata', 'setup_metadata', is_flag=False, default=None,
              help='Metadata for Setup payload')
@click.option('--ab', '--authBearer', 'auth_bearer', is_flag=False, default=None,
              help='Enable Authentication Metadata Extension (Bearer)')
@click.option('--dataMimeType', '--dataFormat', '--dmt', 'data_mime_type', is_flag=False,
              help='MimeType for data (default: application/json)')
@click.option('--metadataMimeType', '--metadataFormat', '--mmt', 'metadata_mime_type', is_flag=False,
              help='MimeType for metadata (default:application/json)')
@click.option('--allowUntrustedSsl', 'allow_untrusted_ssl', is_flag=True, default=False,
              help='Do not verify SSL certificate (for wss:// urls)')
@click.option('-H', '--header', '--httpHeader', 'http_header', multiple=True,
              help='ws/wss headers')
@click.option('--trustCert', 'trust_cert', is_flag=False,
              help='PEM file for a trusted certificate. (e.g. ./foo.crt, /tmp/foo.crt)')
@click.option('--debug', is_flag=True,
              help='Show debug log')
@click.option('--quiet', '-q', is_flag=True,
              help='Disable the output on next')
@click.option('--timeout', 'timeout_seconds', is_flag=False, type=int,
              help='Timeout in seconds')
@click.option('--version', is_flag=True,
              help='Print version')
@click.option('--gql-query', is_flag=False,
              help='GraphQL query/mutate')
@click.option('--gql-subscribe', is_flag=False,
              help='GraphQL subscribe')
@click.option('--gql-schema', is_flag=False,
              help='GraphQL schema file')
@click.argument('uri', required=False)
@click.pass_context
async def command(context, data, load,
                  metadata, route_value, auth_simple, auth_bearer,
                  limit_rate, take_n, allow_untrusted_ssl,
                  setup_data, setup_metadata, interaction_model,
                  http_header, metadata_push, timeout_seconds,
                  data_mime_type, metadata_mime_type,
                  request, stream, channel, fnf, trust_cert,
                  uri, debug, version, quiet,
                  gql_query, gql_subscribe, gql_schema):
    if version:
        try:
            print(get_version('rsocket'))
        except Exception:
            print('Failed to find version')
        return

    if uri is None:
        raise click.MissingParameter(param=context.command.params[-1])

    if quiet:
        logging.basicConfig(handlers=[])

    if debug:
        logging.basicConfig(level=logging.DEBUG)

    if take_n == 0:
        return

    parsed_uri = parse_uri(uri)

    if gql_query is not None or gql_subscribe is not None:
        query = ''
        is_subscription = False
        if gql_query is not None:
            query = gql_query
        elif gql_subscribe is not None:
            query = gql_subscribe
            is_subscription = True

        await run_gql_query(query, is_subscription,
                            allow_untrusted_ssl=allow_untrusted_ssl,
                            metadata_mime_type=metadata_mime_type,
                            data_mime_type=data_mime_type,
                            trust_cert=trust_cert,
                            parsed_uri=parsed_uri)
        return

    request_type = get_request_type(request, stream, fnf, metadata_push, channel, interaction_model)
    http_headers = parse_headers(http_header)
    composite_items = build_composite_metadata(auth_simple, route_value, auth_bearer)
    setup_payload = create_setup_payload(setup_data, setup_metadata)
    metadata_value = get_metadata_value(composite_items, metadata)
    metadata_mime_type = normalize_metadata_mime_type(composite_items, metadata_mime_type)

    def payload_provider():
        return create_request_payload(data, load, metadata_value)

    future = run_request(request_type, limit_rate, payload_provider,
                         http_headers=http_headers,
                         allow_untrusted_ssl=allow_untrusted_ssl,
                         metadata_mime_type=metadata_mime_type,
                         data_mime_type=data_mime_type,
                         setup_payload=setup_payload,
                         trust_cert=trust_cert,
                         parsed_uri=parsed_uri)

    if timeout_seconds is not None:
        result = await asyncio.wait_for(future, timeout_seconds)
    else:
        result = await future

    if not quiet:
        output_result(result)


async def run_gql_query(query: str, is_subscription: bool = False, **kwargs):
    from gql import gql

    client = create_gql_client(**kwargs)

    if is_subscription:
        async for response in client.subscribe_async(
                document=gql(query),
                get_execution_result=True):
            print(response.data)
    else:
        response = await client.execute_async(
            document=gql(query),
            get_execution_result=True)
        print(response.data)


async def run_request(request_type: RequestType,
                      limit_rate: Optional[int],
                      payload_provider: Callable[[], Payload],
                      **kwargs):
    async with create_client(**kwargs) as client:
        return await execute_request(client,
                                     request_type,
                                     normalize_limit_rate(limit_rate),
                                     payload_provider())


def parse_headers(http_headers):
    if safe_len(http_headers) > 0:
        headers = dict()

        for header in http_headers:
            parts = header.split('=', 2)
            headers[parts[0]] = parts[1]

        return headers

    return None


def normalize_metadata_mime_type(composite_items: List, metadata_mime_type):
    if len(composite_items) > 0:
        metadata_mime_type = WellKnownMimeTypes.MESSAGE_RSOCKET_COMPOSITE_METADATA

    return metadata_mime_type


def create_request_payload(data: Optional[str],
                           load: Optional[str],
                           metadata: Optional[bytes]) -> Payload:
    data = normalize_data(data, load)

    return Payload(data, metadata)


def output_result(result):
    if isinstance(result, Payload):
        print(result.data.decode('utf-8'))
    elif isinstance(result, Collection):
        print([p.data.decode('utf-8') for p in result])


async def execute_request(awaitable_client: AwaitableRSocket,
                          request_type: RequestType,
                          limit_rate: int,
                          payload: Payload):
    result = None

    if request_type is RequestType.response:
        result = await awaitable_client.request_response(payload)
    elif request_type is RequestType.stream:
        result = await awaitable_client.request_stream(payload, limit_rate=limit_rate)
    elif request_type is RequestType.channel:
        result = await awaitable_client.request_channel(payload, limit_rate=limit_rate)
    elif request_type is RequestType.fnf:
        await awaitable_client.fire_and_forget(payload)
    elif request_type is RequestType.metadata_push:
        await awaitable_client.metadata_push(payload.metadata)

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
    if limit_rate is None or limit_rate <= 0:
        limit_rate = MAX_REQUEST_N

    return limit_rate


if __name__ == '__main__':
    command()
