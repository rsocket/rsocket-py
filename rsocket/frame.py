import struct
from abc import ABCMeta
from enum import IntEnum

PROTOCOL_MAJOR_VERSION = 1
PROTOCOL_MINOR_VERSION = 0


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
        'length', 'frame_type', 'flags', 'stream_id', 'metadata', 'data',
        'flags_ignore', 'flags_metadata',
        'flags_follows', 'flags_complete', 'metadata_only')

    _FLAG_IGNORE_BIT = 0x200
    _FLAG_METADATA_BIT = 0x100

    def __init__(self, frame_type):
        self.length = 0
        self.frame_type = frame_type
        self.flags = 0
        self.stream_id = 0
        self.metadata = b''
        self.data = b''

        self.flags_ignore = False
        self.flags_metadata = False
        self.flags_follows = False
        self.flags_complete = False

        self.metadata_only = False

    class Parser:

        def __init__(self, buffer, offset, limit):
            self.buffer = buffer
            self.offset = offset
            self.limit = limit

        def parse_header(self):
            available = self.limit - self.offset
            if available < 12:
                raise ParseError('Not enough bytes: {} vs 12'.format(available))
            length, frame_type, flags, stream_id = struct.unpack_from(
                '>IHHI', self.buffer, self.offset)
            self.offset += 12
            return length, frame_type, flags, stream_id

    # noinspection PyAttributeOutsideInit
    def parse_header(self, buffer, offset):
        self.length, = struct.unpack('>I', b'\x00' + buffer[offset:offset + 3])
        self.stream_id, self.frame_type, self.flags = struct.unpack_from(
            '>IBB', buffer, offset + 3)
        self.flags |= (self.frame_type & 3) << 8
        self.frame_type >>= 2
        self.flags_ignore = (self.flags & self._FLAG_IGNORE_BIT) != 0
        self.flags_metadata = (self.flags & self._FLAG_METADATA_BIT) != 0
        return 9

    # noinspection PyAttributeOutsideInit
    def parse_metadata(self, buffer, offset):
        if not self.flags_metadata:
            return 0
        if not self.metadata_only:
            length, = struct.unpack('>I', b'\x00' + buffer[offset:offset + 3])
            offset += 3
        else:
            length = self.length - offset + 3
        self.metadata = buffer[offset:offset+length]
        return length + (0 if self.metadata_only else 3)

    # noinspection PyAttributeOutsideInit
    def parse_data(self, buffer, offset):
        length = self.length - offset + 3
        self.data = buffer[offset:offset+length]
        return length

    @staticmethod
    def pack_string(buffer):
        return struct.pack('b', len(buffer)) + buffer

    def serialize(self, middle=b''):
        self.flags &= ~(self._FLAG_IGNORE_BIT | self._FLAG_METADATA_BIT)
        if self.flags_ignore:
            self.flags |= self._FLAG_IGNORE_BIT
        if self.metadata:
            self.flags_metadata = True
            self.flags |= self._FLAG_METADATA_BIT

        # compute the length of the frame: header + middle + suffix.
        self.length = 9 + len(middle)
        if self.flags_metadata and self.metadata:
            self.length += len(self.metadata)
            if not self.metadata_only:
                self.length += 3
        if not self.metadata_only:
            self.length += len(self.data)

        offset = 0
        buffer = bytearray(self.length)

        buffer[offset:offset+3] = struct.pack('>I', self.length - 3)[1:]
        offset += 3

        struct.pack_into('>I', buffer, offset, self.stream_id)
        offset += 4

        buffer[7] = (self.frame_type << 2) | (self.flags >> 8)
        buffer[8] = self.flags & 0xff
        offset += 2

        buffer[offset:offset+len(middle)] = middle[:]
        offset += len(middle)

        if self.flags_metadata and self.metadata:
            length = len(self.metadata)
            if not self.metadata_only:
                buffer[offset:offset+3] = struct.pack('>I', length)[1:]
                offset += 3
            buffer[offset:offset+length] = self.metadata[:]
            offset += length

        if not self.metadata_only:
            buffer[offset:offset+len(self.data)] = self.data[:]
            offset += len(self.data)

        return bytes(buffer)


class SetupFrame(Frame):
    __slots__ = (
        'major_version', 'minor_version',
        'keep_alive_milliseconds', 'max_lifetime_milliseconds',
        'token_length', 'resume_identification_token',
        'metadata_encoding', 'data_encoding',
        'flags_lease', 'flags_strict', 'flags_resume')

    _FLAG_LEASE_BIT = 0x40
    _FLAG_RESUME_BIT = 0x80

    def __init__(self):
        super().__init__(Type.SETUP)
        self.major_version = PROTOCOL_MAJOR_VERSION
        self.minor_version = PROTOCOL_MINOR_VERSION
        self.flags_lease = False
        self.flags_resume = False
        self.keep_alive_milliseconds = 30000
        self.max_lifetime_milliseconds = 120000

    # noinspection PyAttributeOutsideInit
    def parse(self, buffer, offset):
        offset += self.parse_header(buffer, offset)
        self.flags_lease = (self.flags & self._FLAG_LEASE_BIT) != 0
        self.flags_resume = (self.flags & self._FLAG_RESUME_BIT) != 0

        (self.major_version, self.minor_version,
         self.keep_alive_milliseconds, self.max_lifetime_milliseconds) = (
            struct.unpack_from('>HHII', buffer, offset))
        offset += 12

        if self.flags_resume:
            self.token_length = struct.unpack_from('>H', buffer, offset)
            offset += 2
            self.resume_identification_token = (
                buffer[offset:offset+self.token_length])
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

    def serialize(self, middle=b''):
        self.flags &= ~(self._FLAG_LEASE_BIT | self._FLAG_RESUME_BIT)
        if self.flags_lease:
            self.flags |= self._FLAG_LEASE_BIT
        if self.flags_resume:
            self.flags |= self._FLAG_RESUME_BIT
        middle = struct.pack(
            '>HHII', self.major_version, self.minor_version,
            self.keep_alive_milliseconds, self.max_lifetime_milliseconds)
        if self.flags_resume:
            middle += struct.pack('>H', self.token_length)
            assert len(self.resume_identification_token) == self.token_length
            assert isinstance(self.resume_identification_token, bytes)
            middle += self.resume_identification_token
        middle += self.pack_string(self.metadata_encoding)
        middle += self.pack_string(self.data_encoding)
        return Frame.serialize(self, middle)


class ErrorFrame(Frame):
    __slots__ = 'error_code'

    def __init__(self):
        super().__init__(Type.ERROR)

# noinspection PyAttributeOutsideInit
    def parse(self, buffer, offset):
        offset += self.parse_header(buffer, offset)
        self.error_code, = struct.unpack_from('>I', buffer, offset)
        offset += 4
        offset += self.parse_data(buffer, offset)

    def serialize(self, middle=b''):
        middle = struct.pack('>I', self.error_code)
        return Frame.serialize(self, middle)


class LeaseFrame(Frame):
    __slots__ = ('time_to_live', 'number_of_requests')

    def __init__(self):
        super().__init__(Type.LEASE)
        super().metadata_only = True

    # noinspection PyAttributeOutsideInit
    def parse(self, buffer, offset):
        offset += self.parse_header(buffer, offset)
        self.time_to_live, self.number_of_requests = struct.unpack_from(
            '>II', buffer, offset)
        offset += self.parse_metadata(buffer, offset)

    def serialize(self, middle=b''):
        middle = struct.pack('>II', self.time_to_live, self.number_of_requests)
        return Frame.serialize(self, middle)


class KeepAliveFrame(Frame):
    __slots__ = ('flags_respond', "last_receive_position")

    _FLAG_RESPOND_BIT = 0x80

    def __init__(self):
        super().__init__(Type.KEEPALIVE)
        self.flags_respond = False
        self.last_receive_position = 0

    # noinspection PyAttributeOutsideInit
    def parse(self, buffer, offset):
        offset += self.parse_header(buffer, offset)
        self.flags_respond = (self.flags & self._FLAG_RESPOND_BIT) != 0
        self.last_receive_position = struct.unpack_from('>Q', buffer, offset)[0]
        offset += 8
        offset += self.parse_data(buffer, offset)

    def serialize(self, middle=b''):
        self.flags &= ~self._FLAG_RESPOND_BIT
        if self.flags_respond:
            self.flags |= self._FLAG_RESPOND_BIT
        middle = struct.pack('>Q', self.last_receive_position)
        return Frame.serialize(self, middle)


class RequestFrame(Frame):
    __slots__ = 'flags_follows'

    _FLAG_FOLLOWS_BIT = 0x80

    def __init__(self, frame_type):
        super().__init__(frame_type)
        self.flags_follows = False

    # noinspection PyAttributeOutsideInit
    def parse(self, buffer, offset):
        length = self.parse_header(buffer, offset)
        self.flags_follows = (self.flags & self._FLAG_FOLLOWS_BIT) != 0
        return length

    def serialize(self, middle=b''):
        self.flags &= ~self._FLAG_FOLLOWS_BIT
        if self.flags_follows:
            self.flags |= self._FLAG_FOLLOWS_BIT
        return Frame.serialize(self, middle)


class RequestResponseFrame(RequestFrame):
    __slots__ = ()

    def __init__(self):
        super().__init__(Type.REQUEST_RESPONSE)

    # noinspection PyAttributeOutsideInit
    def parse(self, buffer, offset):
        offset += RequestFrame.parse(self, buffer, offset)
        offset += self.parse_metadata(buffer, offset)
        offset += self.parse_data(buffer, offset)


class RequestFireAndForgetFrame(RequestFrame):
    __slots__ = ()

    def __init__(self):
        super().__init__(Type.REQUEST_FNF)

# noinspection PyAttributeOutsideInit
    def parse(self, buffer, offset):
        offset += RequestFrame.parse(self, buffer, offset)
        offset += self.parse_metadata(buffer, offset)
        offset += self.parse_data(buffer, offset)


class RequestStreamFrame(RequestFrame):
    __slots__ = 'initial_request_n'

    def __init__(self):
        super().__init__(Type.REQUEST_STREAM)
        self.initial_request_n = 0

    # noinspection PyAttributeOutsideInit
    def parse(self, buffer, offset):
        offset += RequestFrame.parse(self, buffer, offset)
        self.initial_request_n, = struct.unpack_from('>I', buffer, offset)
        offset += 4
        offset += self.parse_metadata(buffer, offset)
        offset += self.parse_data(buffer, offset)

    def serialize(self, middle=b''):
        middle = struct.pack('>I', self.initial_request_n)
        return RequestFrame.serialize(self, middle)


class RequestChannelFrame(RequestFrame):
    __slots__ = ('initial_request_n', 'flags_complete', 'flags_follows')

    _FLAG_COMPLETE_BIT = 0x40
    _FLAG_FOLLOWS_BIT = 0x80

    def __init__(self):
        super().__init__(Type.REQUEST_CHANNEL)
        self.flags_complete = False
        self.flags_initial = False

    # noinspection PyAttributeOutsideInit
    def parse(self, buffer, offset):
        offset += RequestFrame.parse(self, buffer, offset)
        self.flags_complete = (self.flags & self._FLAG_COMPLETE_BIT) != 0
        self.flags_follows = (self.flags & self._FLAG_FOLLOWS_BIT) != 0
        self.initial_request_n, = struct.unpack_from('>I', buffer, offset)
        offset += 4
        offset += self.parse_metadata(buffer, offset)
        offset += self.parse_data(buffer, offset)

    def serialize(self, middle=b''):
        middle = struct.pack('>I', self.initial_request_n)
        return RequestFrame.serialize(self, middle)


class RequestNFrame(RequestFrame):
    __slots__ = 'request_n'

    def __init__(self):
        super().__init__(Type.REQUEST_N)

    # noinspection PyAttributeOutsideInit
    def parse(self, buffer, offset):
        offset += self.parse_header(buffer, offset)
        self.request_n = struct.unpack_from('>I', buffer, offset)

    def serialize(self, middle=b''):
        middle = struct.pack('>I', self.request_n)
        return Frame.serialize(self, middle)


class CancelFrame(Frame):

    def __init__(self):
        super().__init__(Type.CANCEL)

    def parse(self, buffer, offset):
        self.parse_header(buffer, offset)


class PayloadFrame(Frame):
    __slots__ = ('flags_follows', 'flags_complete', 'flags_next')

    _FLAG_FOLLOWS_BIT = 0x80
    _FLAG_COMPLETE_BIT = 0x40
    _FLAG_NEXT_BIT = 0x20

    def __init__(self):
        super().__init__(Type.PAYLOAD)
        self.flags_follows = False
        self.flags_complete = False
        self.flags_next = False

    # noinspection PyAttributeOutsideInit
    def parse(self, buffer, offset):
        offset += self.parse_header(buffer, offset)
        self.flags_follows = (self.flags & self._FLAG_FOLLOWS_BIT) != 0
        self.flags_complete = (self.flags & self._FLAG_COMPLETE_BIT) != 0
        self.flags_next = (self.flags & self._FLAG_NEXT_BIT) != 0
        offset += self.parse_metadata(buffer, offset)
        offset += self.parse_data(buffer, offset)

    def serialize(self, middle=b''):
        self.flags &= ~(self._FLAG_FOLLOWS_BIT | self._FLAG_COMPLETE_BIT |
                        self._FLAG_NEXT_BIT)
        if self.flags_follows:
            self.flags |= self._FLAG_FOLLOWS_BIT
        if self.flags_complete:
            self.flags |= self._FLAG_COMPLETE_BIT
        if self.flags_next or self.data or self.metadata:
            self.flags |= self._FLAG_NEXT_BIT
        return Frame.serialize(self)


class MetadataPushFrame(Frame):
    __slots__ = ()

    # noinspection PyDunderSlots
    def __init__(self):
        super().__init__(Type.METADATA_PUSH)
        self.metadata_only = True
        self.flags_metadata = True

    # noinspection PyAttributeOutsideInit
    def parse(self, buffer, offset):
        offset += self.parse_header(buffer, offset)
        offset += self.parse_metadata(buffer, offset)


_table = {
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
    # Type.RESUME: ResumeFrame,
    # Type.RESUME_OK: ResumeOKFrame,
}


def parse(buffer, offset=0):
    # A full header is 3 (length) + 4 (stream) + 2 (type, flags) bytes.
    if len(buffer) < 9:
        raise ParseError('Frame too short: {} bytes'.format(len(buffer)))
    frame_type = struct.unpack_from('>B', buffer, offset + 7)[0] >> 2
    try:
        frame = _table[frame_type]()
        frame.parse(buffer, offset)
        return frame
    except KeyError:
        raise ParseError('Wrong frame type: {}'.format(frame_type))
