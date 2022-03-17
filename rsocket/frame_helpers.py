import struct
from io import BytesIO
from typing import Union, AsyncGenerator

from rsocket.fragment import Fragment

MASK_63_BITS = 0x7FFFFFFFFFFFFFFF


def is_flag_set(flags: int, bit: int) -> bool:
    return (flags & bit) != 0


def pack_position(position: int) -> bytes:
    return struct.pack('>Q', position & MASK_63_BITS)


def unpack_position(chunk: bytes) -> int:
    return struct.unpack('>Q', chunk)[0] & MASK_63_BITS


def pack_24bit_length(item_metadata: bytes) -> bytes:
    return pack_24bit(len(item_metadata))


def pack_24bit(length) -> bytes:
    return struct.pack('>I', length)[1:]


def unpack_24bit(metadata: bytes, offset: int) -> int:
    return struct.unpack('>I', b'\x00' + metadata[offset:offset + 3])[0]


def unpack_32bit(buffer: bytes, offset: int) -> int:
    return struct.unpack_from('>I', buffer, offset)[0]


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


def str_to_bytes(route_path: str) -> bytes:
    return route_path.encode('utf-8')


def ensure_bytes(item: Union[bytes, str]) -> bytes:
    if isinstance(item, str):
        return str_to_bytes(item)

    return item
