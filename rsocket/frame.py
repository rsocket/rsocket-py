import struct
from abc import ABCMeta
from enum import IntEnum
from typing import Tuple

PROTOCOL_MAJOR_VERSION = 1
PROTOCOL_MINOR_VERSION = 0
bits_63_mask = 0x7FFFFFFFFFFFFFFF


class ErrorCode(IntEnum):
    INVALID_SETUP = 0x001,
    UNSUPPORTED_SETUP = 0x002,
    REJECTED_SETUP = 0x003,
    REJECTED_RESUME = 0x004,
    CONNECTION_ERROR = 0x101,
    CONNECTION_ERROR_NO_RETRY = 0x102,
    APPLICATION_ERROR = 0x201,
    REJECTED = 0x202,
    CANCELED = 0x203,
    INVALID = 0x204,
    RESERVED = 0xFFFFFFFF


_FLAG_METADATA_BIT = 0x100
_FLAG_IGNORE_BIT = 0x200
_FLAG_COMPLETE_BIT = 0x40
_FLAG_FOLLOWS_BIT = 0x80
_FLAG_LEASE_BIT = 0x40
_FLAG_RESUME_BIT = 0x80
_FLAG_RESPOND_BIT = 0x80


class ParseError(ValueError):
    pass


class Type(IntEnum):
    SETUP = 1,
    LEASE = 2,
    KEEPALIVE = 3,
    REQUEST_RESPONSE = 4,
    REQUEST_FNF = 5,
    REQUEST_STREAM = 6,
    REQUEST_CHANNEL = 7,
    REQUEST_N = 8,
    CANCEL = 9,
    PAYLOAD = 10,
    ERROR = 11,
    METADATA_PUSH = 12,
    RESUME = 13,
    RESUME_OK = 14,
    EXT = 0xFFFF


class Frame(metaclass=ABCMeta):
    __slots__ = (
        'length',
        'frame_type',
        'stream_id',
        'metadata',
        'data',
        'flags_ignore',
        'flags_metadata',
        'flags_follows',
        'flags_complete',
        'metadata_only'
    )

    def __init__(self, frame_type: Type):
        self.length = 0
        self.frame_type = frame_type
        self.stream_id = 0
        self.metadata = b''
        self.data = b''

        self.flags_ignore = False
        self.flags_metadata = False
        self.flags_follows = False
        self.flags_complete = False

        self.metadata_only = False

    def parse_header(self, buffer: bytes, offset: int) -> Tuple[int, int]:
        self.length, = struct.unpack('>I', b'\x00' + buffer[offset:offset + 3])
        self.stream_id, self.frame_type, flags = struct.unpack_from(
            '>IBB', buffer, offset + 3)
        flags |= (self.frame_type & 3) << 8
        self.frame_type >>= 2
        self.flags_ignore = self._is_flag_set(flags, _FLAG_IGNORE_BIT)
        self.flags_metadata = self._is_flag_set(flags, _FLAG_METADATA_BIT)
        return 9, flags

    def _is_flag_set(self, flags: int, bit: int) -> bool:
        return (flags & bit) != 0

    def parse_metadata(self, buffer: bytes, offset: int) -> int:
        if not self.flags_metadata:
            return 0

        if not self.metadata_only:
            length, = struct.unpack('>I', b'\x00' + buffer[offset:offset + 3])
            offset += 3
        else:
            length = self.length - offset + 3

        self.metadata = buffer[offset:offset + length]

        return length + (0 if self.metadata_only else 3)

    def parse_data(self, buffer: bytes, offset: int) -> int:
        length = self.length - offset + 3
        self.data = buffer[offset:offset + length]
        return length

    @staticmethod
    def pack_string(buffer):
        return struct.pack('b', len(buffer)) + buffer

    def serialize(self, middle=b'', flags: int = 0) -> bytes:
        flags &= ~(_FLAG_IGNORE_BIT | _FLAG_METADATA_BIT)
        if self.flags_ignore:
            flags |= _FLAG_IGNORE_BIT
        if self.metadata:
            self.flags_metadata = True
            flags |= _FLAG_METADATA_BIT

        self.length = self._compute_frame_length(middle)

        offset = 0
        buffer = bytearray(self.length)

        buffer[offset:offset + 3] = struct.pack('>I', self.length - 3)[1:]
        offset += 3

        struct.pack_into('>I', buffer, offset, self.stream_id)
        offset += 4

        buffer[7] = (self.frame_type << 2) | (flags >> 8)
        buffer[8] = flags & 0xff
        offset += 2

        buffer[offset:offset + len(middle)] = middle[:]
        offset += len(middle)

        if self.flags_metadata and self.metadata:
            length = len(self.metadata)
            if not self.metadata_only:
                buffer[offset:offset + 3] = struct.pack('>I', length)[1:]
                offset += 3
            buffer[offset:offset + length] = self.metadata[:]
            offset += length

        if not self.metadata_only and self.data:
            buffer[offset:offset + len(self.data)] = self.data[:]
            offset += len(self.data)

        return bytes(buffer)

    def _compute_frame_length(self, middle: bytes) -> int:
        header_length = 9
        length = header_length + len(middle)

        if self.flags_metadata and self.metadata:
            length += len(self.metadata)
            if not self.metadata_only:
                length += 3

        if not self.metadata_only and self.data:
            length += len(self.data)

        return length


class SetupFrame(Frame):
    __slots__ = (
        'major_version',
        'minor_version',
        'keep_alive_milliseconds',
        'max_lifetime_milliseconds',
        'token_length',
        'resume_identification_token',
        'metadata_encoding',
        'data_encoding',
        'flags_lease',
        'flags_resume'
    )

    def __init__(self):
        super().__init__(Type.SETUP)
        self.major_version = PROTOCOL_MAJOR_VERSION
        self.minor_version = PROTOCOL_MINOR_VERSION
        self.flags_lease = False
        self.flags_resume = False

    # noinspection PyAttributeOutsideInit
    def parse(self, buffer: bytes, offset: int):
        header = self.parse_header(buffer, offset)
        offset += header[0]
        flags = header[1]
        self.flags_lease = self._is_flag_set(flags, _FLAG_LEASE_BIT)
        self.flags_resume = self._is_flag_set(flags, _FLAG_RESUME_BIT)

        (self.major_version, self.minor_version,
         self.keep_alive_milliseconds, self.max_lifetime_milliseconds) = (
            struct.unpack_from('>HHII', buffer, offset))

        offset += 12

        if self.flags_resume:
            self.token_length = struct.unpack_from('>H', buffer, offset)[0]
            offset += 2
            self.resume_identification_token = (
                buffer[offset:offset + self.token_length])
            offset += self.token_length

        def unpack_string():
            nonlocal offset
            length, = struct.unpack_from('b', buffer, offset)
            result = buffer[offset + 1:offset + length + 1]
            offset += length + 1
            return result

        self.metadata_encoding = unpack_string()
        self.data_encoding = unpack_string()

        offset += self.parse_metadata(buffer, offset)
        offset += self.parse_data(buffer, offset)

    def serialize(self, middle=b'', flags=0) -> bytes:
        flags &= ~(_FLAG_LEASE_BIT | _FLAG_RESUME_BIT)
        if self.flags_lease:
            flags |= _FLAG_LEASE_BIT
        if self.flags_resume:
            flags |= _FLAG_RESUME_BIT
        middle = struct.pack(
            '>HHII', self.major_version, self.minor_version,
            self.keep_alive_milliseconds, self.max_lifetime_milliseconds)
        if self.flags_resume:
            middle += struct.pack('>H', self.token_length)
            # assert len(self.resume_identification_token) == self.token_length
            # assert isinstance(self.resume_identification_token, bytes)
            middle += self.resume_identification_token
        middle += self.pack_string(self.metadata_encoding)
        middle += self.pack_string(self.data_encoding)
        return Frame.serialize(self, middle, flags)


class ErrorFrame(Frame):
    __slots__ = 'error_code'

    def __init__(self):
        super().__init__(Type.ERROR)

    # noinspection PyAttributeOutsideInit
    def parse(self, buffer: bytes, offset: int):
        header = self.parse_header(buffer, offset)
        offset += header[0]
        flags = header[1]
        self.error_code, = struct.unpack_from('>I', buffer, offset)
        offset += 4
        offset += self.parse_data(buffer, offset)

    def serialize(self, middle=b'', flags=0) -> bytes:
        middle = struct.pack('>I', self.error_code)
        return Frame.serialize(self, middle, flags)


class LeaseFrame(Frame):
    __slots__ = (
        'time_to_live',
        'number_of_requests'
    )

    def __init__(self):
        super().__init__(Type.LEASE)
        super().metadata_only = True

    # noinspection PyAttributeOutsideInit
    def parse(self, buffer, offset):
        offset += self.parse_header(buffer, offset)
        self.time_to_live, self.number_of_requests = struct.unpack_from(
            '>II', buffer, offset)
        offset += self.parse_metadata(buffer, offset)

    def serialize(self, middle=b'', flags=0):
        middle = struct.pack('>II', self.time_to_live, self.number_of_requests)
        return Frame.serialize(self, middle, flags)


class KeepAliveFrame(Frame):
    __slots__ = ('flags_respond', 'last_received_position')

    def __init__(self, data=b'', metadata=b''):
        super().__init__(Type.KEEPALIVE)
        self.flags_respond = False
        self.data = data
        self.metadata = metadata

    def parse(self, buffer: bytes, offset: int):
        header = self.parse_header(buffer, offset)
        offset += header[0]
        flags = header[1]
        self.flags_respond = self._is_flag_set(flags, _FLAG_RESPOND_BIT)
        offset += self.parse_data(buffer, offset)
        # self.last_received_position = struct.unpack('>Q', buffer[offset:])[0]

    def serialize(self, middle=b'', flags: int = 0) -> bytes:
        flags &= ~_FLAG_RESPOND_BIT
        if self.flags_respond:
            flags |= _FLAG_RESPOND_BIT
        # middle += struct.pack('>Q', self.last_received_position)
        return Frame.serialize(self, middle, flags)


class RequestFrame(Frame):
    __slots__ = (
        'flags_follows',
    )

    _FLAG_FOLLOWS_BIT = 0x80

    def __init__(self, frame_type):
        super().__init__(frame_type)
        self.flags_follows = False

    def parse(self, buffer, offset: int) -> Tuple[int, int]:
        length, flags = self.parse_header(buffer, offset)
        self.flags_follows = self._is_flag_set(flags, self._FLAG_FOLLOWS_BIT)
        return length, flags

    def serialize(self, middle=b'', flags: int = 0) -> bytes:
        flags &= ~self._FLAG_FOLLOWS_BIT

        if self.flags_follows:
            flags |= self._FLAG_FOLLOWS_BIT

        return Frame.serialize(self, middle, flags)

    def _parse_payload(self, buffer: bytes, offset: int):
        offset += self.parse_metadata(buffer, offset)
        offset += self.parse_data(buffer, offset)


class RequestResponseFrame(RequestFrame):
    __slots__ = ()

    def __init__(self):
        super().__init__(Type.REQUEST_RESPONSE)

    # noinspection PyAttributeOutsideInit
    def parse(self, buffer, offset):
        header = RequestFrame.parse(self, buffer, offset)
        offset += header[0]
        flags = header[1]
        self._parse_payload(buffer, offset)


class RequestFireAndForgetFrame(RequestFrame):
    __slots__ = ()

    def __init__(self):
        super().__init__(Type.REQUEST_FNF)

    def parse(self, buffer, offset):
        header = RequestFrame.parse(self, buffer, offset)
        offset += header[0]
        self._parse_payload(buffer, offset)


class RequestStreamFrame(RequestFrame):
    __slots__ = 'initial_request_n'

    def __init__(self):
        super().__init__(Type.REQUEST_STREAM)
        self.initial_request_n = 0

    def parse(self, buffer, offset):
        header = RequestFrame.parse(self, buffer, offset)
        offset += header[0]
        self.initial_request_n, = struct.unpack_from('>I', buffer, offset)
        offset += 4
        self._parse_payload(buffer, offset)

    def serialize(self, middle=b'', flags=0):
        middle = struct.pack('>I', self.initial_request_n)
        return RequestFrame.serialize(self, middle)


class RequestChannelFrame(RequestFrame):
    __slots__ = (
        'initial_request_n',
        'flags_complete',
        'flags_follows',
        'flags_initial'
    )

    def __init__(self):
        super().__init__(Type.REQUEST_CHANNEL)
        self.flags_complete = False
        self.flags_initial = False

    # noinspection PyAttributeOutsideInit
    def parse(self, buffer, offset):
        header = RequestFrame.parse(self, buffer, offset)
        offset += header[0]
        flags = header[1]
        self.flags_complete = self._is_flag_set(flags, _FLAG_COMPLETE_BIT)
        self.flags_follows = self._is_flag_set(flags, _FLAG_FOLLOWS_BIT)
        self.initial_request_n, = struct.unpack_from('>I', buffer, offset)
        offset += 4
        self._parse_payload(buffer, offset)

    def serialize(self, middle=b'', flags=0):
        middle = struct.pack('>I', self.initial_request_n)
        return RequestFrame.serialize(self, middle)


class RequestNFrame(RequestFrame):
    __slots__ = 'request_n'

    def __init__(self):
        super().__init__(Type.REQUEST_N)

    # noinspection PyAttributeOutsideInit
    def parse(self, buffer, offset):
        header = self.parse_header(buffer, offset)
        offset += header[0]
        self.request_n, = struct.unpack_from('>I', buffer, offset)

    def serialize(self, middle=b'', flags=0):
        middle = struct.pack('>I', self.request_n)
        return Frame.serialize(self, middle, flags)


class CancelFrame(Frame):

    def __init__(self):
        super().__init__(Type.CANCEL)

    def parse(self, buffer, offset):
        self.parse_header(buffer, offset)


class PayloadFrame(Frame):
    __slots__ = (
        'flags_follows',
        'flags_complete',
        'flags_next'
    )

    _FLAG_FOLLOWS_BIT = 0x80
    _FLAG_COMPLETE_BIT = 0x40
    _FLAG_NEXT_BIT = 0x20

    def __init__(self):
        super().__init__(Type.PAYLOAD)
        self.flags_follows = False
        self.flags_complete = False
        self.flags_next = False

    def parse(self, buffer, offset):
        header = self.parse_header(buffer, offset)
        offset += header[0]
        flags = header[1]
        self.flags_follows = self._is_flag_set(flags, self._FLAG_FOLLOWS_BIT)
        self.flags_complete = self._is_flag_set(flags, self._FLAG_COMPLETE_BIT)
        self.flags_next = self._is_flag_set(flags, self._FLAG_NEXT_BIT)
        offset += self.parse_metadata(buffer, offset)
        offset += self.parse_data(buffer, offset)

    def serialize(self, middle=b'', flags=0):
        flags &= ~(self._FLAG_FOLLOWS_BIT | self._FLAG_COMPLETE_BIT |
                   self._FLAG_NEXT_BIT)
        if self.flags_follows:
            flags |= self._FLAG_FOLLOWS_BIT
        if self.flags_complete:
            flags |= self._FLAG_COMPLETE_BIT
        if self.flags_next:
            flags |= self._FLAG_NEXT_BIT
        return Frame.serialize(self, flags=flags)


class MetadataPushFrame(Frame):
    __slots__ = ()

    def __init__(self):
        super().__init__(Type.METADATA_PUSH)
        self.metadata_only = True
        self.flags_metadata = True

    def parse(self, buffer, offset):
        offset += self.parse_header(buffer, offset)
        offset += self.parse_metadata(buffer, offset)


class ResumeFrame(Frame):
    __slots__ = (
        'major_version',
        'minor_version',
        'token_length',
        'resume_identification_token',
        'last_server_position',
        'first_client_position'
    )

    def __init__(self):
        super().__init__(Type.RESUME)
        self.major_version = PROTOCOL_MAJOR_VERSION
        self.minor_version = PROTOCOL_MINOR_VERSION

    # noinspection PyAttributeOutsideInit
    def parse(self, buffer: bytes, offset: int):
        header = self.parse_header(buffer, offset)
        offset += header[0]

        (self.major_version, self.minor_version) = (
            struct.unpack_from('>HH', buffer, offset))

        offset += 4

        self.token_length = struct.unpack_from('>H', buffer, offset)[0]
        offset += 2
        self.resume_identification_token = (
            buffer[offset:offset + self.token_length])
        offset += self.token_length

        self.last_server_position = struct.unpack('>Q', buffer[offset:offset + 8])[0] & bits_63_mask
        offset += 8
        self.first_client_position = struct.unpack('>Q', buffer[offset:])[0] & bits_63_mask

    def serialize(self, middle=b'', flags=0) -> bytes:
        flags &= ~(_FLAG_LEASE_BIT | _FLAG_RESUME_BIT)

        middle = struct.pack('>HH', self.major_version, self.minor_version)
        middle += struct.pack('>H', len(self.resume_identification_token))
        # assert len(self.resume_identification_token) == self.token_length
        # assert isinstance(self.resume_identification_token, bytes)
        middle += self.resume_identification_token

        middle += struct.pack('>Q', self.last_server_position & bits_63_mask)
        middle += struct.pack('>Q', self.first_client_position & bits_63_mask)

        return Frame.serialize(self, middle)


class ResumeOKFrame(Frame):
    ...


_frame_class_by_id = {
    Type.SETUP: SetupFrame,
    Type.LEASE: LeaseFrame,
    Type.KEEPALIVE: KeepAliveFrame,
    Type.REQUEST_RESPONSE: RequestResponseFrame,
    Type.REQUEST_FNF: RequestFireAndForgetFrame,
    Type.REQUEST_STREAM: RequestStreamFrame,
    Type.REQUEST_CHANNEL: RequestChannelFrame,
    Type.REQUEST_N: RequestNFrame,
    Type.CANCEL: CancelFrame,
    Type.PAYLOAD: PayloadFrame,
    Type.ERROR: ErrorFrame,
    Type.METADATA_PUSH: MetadataPushFrame,
    Type.RESUME: ResumeFrame,
    Type.RESUME_OK: ResumeOKFrame,
}


def parse(buffer: bytes, offset=0) -> Frame:
    if len(buffer) < 9:  # A full header is 3 (length) + 4 (stream) + 2 (type, flags) bytes.
        raise ParseError('Frame too short: {} bytes'.format(len(buffer)))

    frame_type = struct.unpack_from('>B', buffer, offset + 7)[0] >> 2

    try:
        frame = _frame_class_by_id[frame_type]()
        frame.parse(buffer, offset)
        return frame
    except KeyError:
        raise ParseError('Wrong frame type: {}'.format(frame_type))


def is_fragmentable_frame(frame: Frame) -> bool:
    return isinstance(frame, (
        PayloadFrame,
        RequestResponseFrame,
        RequestChannelFrame,
        RequestStreamFrame,
        RequestFireAndForgetFrame
    ))
