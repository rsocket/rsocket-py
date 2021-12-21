from typing import Union


def data_bits(data: Union[str, bytes]):
    if isinstance(data, str):
        byte_array = data.encode()
        binary_int = int.from_bytes(byte_array, "big")
        return bin(binary_int)[2:]
    if isinstance(data, bytes):
        return ''.join(format(byte, '08b') for byte in data)


def build_frame(*items) -> bytes:
    bits = ''.join(items)
    bits_length = len(bits)
    nearest_round_length = int(8 * round(bits_length / 8.))
    bits = bits.ljust(nearest_round_length, '0')
    return bitstring_to_bytes(bits)


def to_hex_string(x):
    return ''.join(r'\x' + hex(letter)[2:].zfill(2) for letter in x)


def bitstring_to_bytes(s: str) -> bytes:
    return int(s, 2).to_bytes((len(s) + 7) // 8, byteorder='big')


def bits(bit_count, value, comment) -> str:
    return f'{value:b}'.zfill(bit_count)
