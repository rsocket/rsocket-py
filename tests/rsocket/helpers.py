from math import ceil
from typing import Type

from rsocket.helpers import create_future
from rsocket.logger import logger
from rsocket.payload import Payload
from rsocket.request_handler import BaseRequestHandler
from rsocket.rsocket_base import RSocketBase


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
    return create_future(Payload(b'data: ' + request.data,
                                 b'meta: ' + request.metadata))


def assert_no_open_streams(client: RSocketBase, server: RSocketBase):
    logger().info('Checking for open client streams')

    assert len(client._stream_control._streams) == 0, 'Client has open streams'

    logger().info('Checking for open server streams')

    assert len(server._stream_control._streams) == 0, 'Server has open streams'


class IdentifiedHandler(BaseRequestHandler):
    def __init__(self, socket, server_id: int):
        super().__init__(socket)
        self._server_id = server_id


class IdentifiedHandlerFactory:
    def __init__(self, server_id: int, handler_factory: Type[IdentifiedHandler]):
        self._server_id = server_id
        self._handler_factory = handler_factory

    def factory(self, socket) -> BaseRequestHandler:
        return self._handler_factory(socket, self._server_id)


def force_closing_connection(current_connection):
    current_connection[1].close()
