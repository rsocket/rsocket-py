import struct
from datetime import timedelta
from typing import Union, Callable, Optional, TypeVar, Tuple


def to_milliseconds(period: timedelta) -> int:
    return round(period.total_seconds() * 1000) + round(period.microseconds / 1000)


async def noop_frame_handler(frame):
    pass


async def always_allow_authenticator(authentication):
    pass


def str_to_bytes(route_path: str):
    return bytes(bytearray(map(ord, route_path)))


def ensure_bytes(item: Union[bytes, str]) -> bytes:
    if isinstance(item, str):
        return str_to_bytes(item)
    return item


T = TypeVar('T')
V = TypeVar('V')


def serialize_well_known_encoding(encoding: bytes, encoding_parser: Callable[[bytes], Optional[T]]) -> bytes:
    known_type = encoding_parser(encoding)

    if known_type is None:
        encoding_length = len(encoding)

        if encoding_length > 0b1111111:
            raise Exception('metadata encoding type too long')

        serialized = ((0 << 7) | encoding_length & 0b1111111).to_bytes(1, 'big')
        serialized += encoding
    else:
        serialized = ((1 << 7) | known_type.id & 0b1111111).to_bytes(1, 'big')

    return serialized


def parse_well_known_encoding(buffer: bytes, encoding_name_provider: Callable[[T], V]) -> Tuple[bytes, int]:
    is_known_mime_id = struct.unpack('>B', buffer[:1])[0] >> 7 == 1
    mime_length_or_type = (struct.unpack('>B', buffer[:1])[0]) & 0b1111111
    if is_known_mime_id:
        metadata_encoding = encoding_name_provider(mime_length_or_type).name
        offset = 1
    else:
        metadata_encoding = buffer[1:1 + mime_length_or_type]
        offset = 1 + mime_length_or_type

    return metadata_encoding, offset