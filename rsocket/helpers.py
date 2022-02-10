import struct
from datetime import timedelta
from io import BytesIO
from typing import Union, Callable, Optional, TypeVar, Tuple, AsyncGenerator

from rsocket.fragment import Fragment


def to_milliseconds(period: timedelta) -> int:
    return round(period.total_seconds() * 1000) + round(period.microseconds / 1000)


async def noop_frame_handler(frame):
    pass


def str_to_bytes(route_path: str) -> bytes:
    return route_path.encode('utf-8')


def ensure_bytes(item: Union[bytes, str]) -> bytes:
    if isinstance(item, str):
        return str_to_bytes(item)

    return item


T = TypeVar('T')
V = TypeVar('V')


def serialize_well_known_encoding(
        encoding: bytes,
        encoding_parser: Optional[Callable[[bytes], Optional[T]]] = None) -> bytes:
    known_type = None

    if encoding_parser is not None:
        known_type = encoding_parser(encoding)

    if known_type is None:
        encoding_length = len(encoding)
        encoded_encoding_length = encoding_length - 1  # mime length cannot be 0

        if encoded_encoding_length > 0b1111111:
            raise Exception('metadata encoding type too long')

        serialized = ((0 << 7) | encoded_encoding_length & 0b1111111).to_bytes(1, 'big')
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
        real_mime_type_length = mime_length_or_type + 1  # mime length cannot be 0
        metadata_encoding = bytes(buffer[1:1 + real_mime_type_length])
        offset = 1 + real_mime_type_length

    return metadata_encoding, offset


async def payload_to_n_size_fragments(data_reader: BytesIO,
                                      metadata_reader: BytesIO,
                                      fragment_size: int
                                      ) -> AsyncGenerator[Fragment, None]:
    while True:
        metadata_fragment = metadata_reader.read(fragment_size)

        if len(metadata_fragment) == 0:
            last_metadata_fragment = b''
            break

        if len(metadata_fragment) < fragment_size:
            last_metadata_fragment = metadata_fragment
            break
        else:
            yield Fragment(None, metadata_fragment, is_last=False)

    expected_data_fragment_length = fragment_size - len(last_metadata_fragment)
    data_fragment = data_reader.read(expected_data_fragment_length)

    if len(last_metadata_fragment) > 0 or len(data_fragment) > 0:
        last_fragment_sent = len(data_fragment) < expected_data_fragment_length
        yield Fragment(data_fragment, last_metadata_fragment, is_last=last_fragment_sent)

        if last_fragment_sent:
            return

    if len(data_fragment) == 0:
        yield Fragment(None, None, is_last=True)
        return

    while True:
        data_fragment = data_reader.read(fragment_size)

        is_last_fragment = len(data_fragment) < fragment_size
        yield Fragment(data_fragment, None, is_last=is_last_fragment)
        if is_last_fragment:
            break
