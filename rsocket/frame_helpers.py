"""
Low level helpers. Other than Exception classes must not import anything else from rsocket package
to avoid circular dependencies.
"""

import struct
from typing import Union, Tuple, Optional

from rsocket.exceptions import RSocketMimetypeTooLong

MASK_63_BITS = 0x7FFFFFFFFFFFFFFF


def is_flag_set(flags: int, bit: int) -> bool:
    return (flags & bit) != 0


def pack_string(buffer: bytes) -> bytes:
    return struct.pack('b', len(buffer)) + buffer


def unpack_string(buffer: bytes, offset: int) -> Tuple[int, bytes]:
    length = struct.unpack_from('b', buffer, offset)[0]
    result = buffer[offset + 1:offset + length + 1]
    return length, result


def pack_24bit_length(item_metadata: bytes) -> bytes:
    return pack_24bit(len(item_metadata))


def unpack_32bit(buffer: bytes, offset: int) -> int:
    return struct.unpack_from('>I', buffer, offset)[0]


def str_to_bytes(string: str) -> bytes:
    return string.encode('utf-8')


def ensure_bytes(item: Optional[Union[bytes, str]]) -> Optional[bytes]:
    if isinstance(item, str):
        return str_to_bytes(item)

    return item


def serialize_128max_value(encoding: bytes) -> bytes:
    encoding_length = len(encoding)
    encoded_encoding_length = encoding_length - 1  # mime length cannot be 0

    if encoded_encoding_length > 0b1111111:
        raise RSocketMimetypeTooLong(encoding)

    serialized = ((0 << 7) | encoded_encoding_length & 0b1111111).to_bytes(1, 'big')
    serialized += encoding
    return serialized


def safe_len(value) -> int:
    if value is None:
        return 0

    return len(value)


try:
    import cbitstruct


    def parse_type(buffer: bytes) -> Tuple[int, int]:
        return cbitstruct.unpack('u1u7', buffer[:1])


    def unpack_position(chunk: bytes) -> int:
        return cbitstruct.unpack('u1u63', chunk)[1]


    def unpack_24bit(metadata: bytes, offset: int) -> int:
        return cbitstruct.unpack('u24', metadata[offset:offset + 3])[0]


    def pack_position(position: int) -> bytes:
        return cbitstruct.pack('u1u63', 0, position)


    def pack_24bit(length) -> bytes:
        return cbitstruct.pack('u24', length)


except ImportError:
    def parse_type(buffer: bytes) -> Tuple[int, int]:
        data_byte = struct.unpack('>B', buffer[:1])[0]
        is_known_type = data_byte >> 7 == 1
        length_or_type = data_byte & 0b1111111
        return is_known_type, length_or_type


    def unpack_position(chunk: bytes) -> int:
        return struct.unpack('>Q', chunk)[0] & MASK_63_BITS


    def unpack_24bit(metadata: bytes, offset: int) -> int:
        return struct.unpack('>I', b'\x00' + metadata[offset:offset + 3])[0]


    def pack_position(position: int) -> bytes:
        return struct.pack('>Q', position & MASK_63_BITS)


    def pack_24bit(length) -> bytes:
        return struct.pack('>I', length)[1:]
