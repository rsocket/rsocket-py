import struct

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


def unpack_32bit(buffer:bytes, offset:int) -> int:
    return struct.unpack_from('>I', buffer, offset)[0]
