from math import ceil


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
