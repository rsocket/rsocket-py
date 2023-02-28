import asyncio
import json
import os
from contextlib import contextmanager
from dataclasses import dataclass
from datetime import timedelta, datetime
from math import ceil
from typing import Tuple, Any
from typing import Type, Callable

from rsocket.frame_helpers import str_to_bytes, ensure_bytes
from rsocket.helpers import create_future, noop
from rsocket.logger import logger
from rsocket.payload import Payload
from rsocket.request_handler import BaseRequestHandler, RequestHandler
from rsocket.rsocket_base import RSocketBase
from rsocket.rsocket_client import RSocketClient
from rsocket.rsocket_server import RSocketServer
from rsocket.transports.transport import Transport


def data_bits(data: bytes, name: str = None):
    return ''.join(format(byte, '08b') for byte in data)


def build_frame(*items) -> bytes:
    frame_bits = ''.join(items)
    bits_length = len(frame_bits)
    nearest_round_length = int(8 * ceil(bits_length / 8.))
    frame_bits = frame_bits.ljust(nearest_round_length, '0')
    return bitstring_to_bytes(frame_bits)


def bitstring_to_bytes(s: str) -> bytes:
    return int(s, 2).to_bytes((len(s) + 7) // 8, byteorder='big')


def bits(bit_count, value, comment) -> str:
    return f'{value:b}'.zfill(bit_count)


def future_from_payload(request: Payload):
    return create_future(to_test_response_payload(request))


def to_test_response_payload(request):
    return Payload(b'data: ' + request.data,
                   b'meta: ' + request.metadata)


def assert_no_open_streams(client: RSocketBase, server: RSocketBase):
    logger().info('Checking for open client streams')

    if client is not None:
        assert len(client._stream_control._streams) == 0, 'Client has open streams'

    logger().info('Checking for open server streams')

    if server is not None:
        assert len(server._stream_control._streams) == 0, 'Server has open streams'


class IdentifiedHandler(BaseRequestHandler):
    def __init__(self, server_id: int, delay=timedelta(0)):
        self._delay = delay
        self._server_id = server_id


class IdentifiedHandlerFactory:
    def __init__(self,
                 server_id: int,
                 handler_factory: Type[IdentifiedHandler],
                 delay=timedelta(0),
                 on_handler_create: Callable[[RequestHandler], None] = noop):
        self._on_handler_create = on_handler_create
        self._delay = delay
        self._server_id = server_id
        self._handler_factory = handler_factory

    def factory(self) -> BaseRequestHandler:
        handler = self._handler_factory(self._server_id, self._delay)
        self._on_handler_create(handler)
        return handler


async def force_closing_connection(transport, delay=timedelta(0)):
    await asyncio.sleep(delay.total_seconds())
    await transport.close()


@dataclass
class ServerContainer:
    server: RSocketServer = None
    transport: Transport = None


def get_components(pipe) -> Tuple[RSocketServer, RSocketClient]:
    return pipe


def to_json_bytes(item: Any) -> bytes:
    return str_to_bytes(json.dumps(item))


def create_data(base: bytes, multiplier: int, limit: float = None):
    return b''.join([ensure_bytes(str(i)) + base for i in range(multiplier)])[0:limit]


def create_large_random_data(size: int):
    return bytearray(os.urandom(size))


def benchmark_method(method: Callable, iterations: int) -> float:
    with measure_runtime() as result:
        for i in range(iterations):
            method()

    return result.time.total_seconds() * 1000 / iterations


@dataclass
class Result:
    time: timedelta = None


@contextmanager
def measure_runtime():
    result = Result()
    start = datetime.now()
    yield result
    result.time = datetime.now() - start
