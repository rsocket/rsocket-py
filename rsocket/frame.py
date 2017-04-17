import struct
from abc import ABCMeta
from enum import IntEnum

PROTOCOL_MAJOR_VERSION = 0
PROTOCOL_MINOR_VERSION = 2


class ErrorCode(IntEnum):
    INVALID_SETUP = 1,
    UNSUPPORTED_SETUP = 2,
    REJECTED_SETUP = 3,
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
    REQUEST_SUB = 7,  # delete this, and move below up one.
    REQUEST_CHANNEL = 8,
    REQUEST_N = 9,
    CANCEL = 10,
    RESPONSE = 11,
    ERROR = 12,
    METADATA_PUSH = 13,
    RESUME = 14,
    RESUME_OK = 15,
    EXT = 0xFFFF


class Frame(metaclass=ABCMeta):
    __slots__ = (
        'length', 'frame_type', 'flags', 'stream_id', 'metadata', 'data',
        'flags_ignore', 'flags_metadata',
        'flags_follows', 'flags_complete')

    _FLAG_IGNORE_BIT = 0x8000
    _FLAG_METADATA_BIT = 0x4000

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
        self.length, self.frame_type, self.flags, self.stream_id = (
            struct.unpack_from('>IHHI', buffer, offset))
        self.flags_ignore = (self.flags & self._FLAG_IGNORE_BIT) != 0
        self.flags_metadata = (self.flags & self._FLAG_METADATA_BIT) != 0
        return 12

    # noinspection PyAttributeOutsideInit
    def parse_metadata(self, buffer, offset):
        if not self.flags_metadata:
            return 0
        length, = struct.unpack_from('>I', buffer, offset)
        self.metadata = buffer[offset+4:offset + length]
        return length

    # noinspection PyAttributeOutsideInit
    def parse_data(self, buffer, offset):
        length = self.length - offset
        self.data = buffer[offset:self.length]
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
        self.length = 12 + len(middle) + len(self.data)
        if self.flags_metadata and self.metadata:
            self.length += 4 + len(self.metadata)

        offset = 0
        buffer = bytearray(self.length)

        struct.pack_into('>IHHI', buffer, offset, self.length, self.frame_type,
                         self.flags, self.stream_id)
        offset += 12

        buffer[offset:offset+len(middle)] = middle[:]
        offset += len(middle)

        if self.flags_metadata and self.metadata:
            struct.pack_into('>I', buffer, offset, 4 + len(self.metadata))
            offset += 4
            buffer[offset:offset+len(self.metadata)] = self.metadata[:]
            offset += len(self.metadata)
        buffer[offset:offset+len(self.data)] = self.data[:]
        offset += len(self.data)
        return bytes(buffer)


class SetupFrame(Frame):
    __slots__ = (
        'major_version', 'minor_version', 'keep_alive_milliseconds',
        'max_lifetime_milliseconds', 'metadata_encoding', 'data_encoding',
        'resume_identification_token',
        'flags_lease', 'flags_strict', 'flags_resume')

    _FLAG_LEASE_BIT = 0x2000
    _FLAG_STRICT_BIT = 0x1000
    _FLAG_RESUME_BIT = 0x0800

    def __init__(self):
        super().__init__(Type.SETUP)
        self.major_version = PROTOCOL_MAJOR_VERSION
        self.minor_version = PROTOCOL_MINOR_VERSION
        self.flags_lease = False
        self.flags_strict = False
        self.flags_resume = False

    # noinspection PyAttributeOutsideInit
    def parse(self, buffer, offset):
        offset += self.parse_header(buffer, offset)
        self.flags_lease = (self.flags & self._FLAG_LEASE_BIT) != 0
        self.flags_strict = (self.flags & self._FLAG_STRICT_BIT) != 0
        self.flags_resume = (self.flags & self._FLAG_RESUME_BIT) != 0

        (self.major_version, self.minor_version,
         self.keep_alive_milliseconds, self.max_lifetime_milliseconds) = (
            struct.unpack_from('>HHII', buffer, offset))
        offset += 12

        if self.flags_resume:
            self.resume_identification_token = buffer[offset:offset + 16]
            offset += 16

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
        self.flags &= ~(self._FLAG_LEASE_BIT | self._FLAG_STRICT_BIT |
                        self._FLAG_RESUME_BIT)
        if self.flags_lease:
            self.flags |= self._FLAG_LEASE_BIT
        if self.flags_strict:
            self.flags |= self._FLAG_STRICT_BIT
        if self.flags_resume:
            self.flags |= self._FLAG_RESUME_BIT
        middle = struct.pack(
            '>HHII', self.major_version, self.minor_version,
            self.keep_alive_milliseconds, self.max_lifetime_milliseconds)
        if self.flags_resume:
            assert len(self.resume_identification_token) == 16
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
        self.error_code = struct.unpack_from('>I', buffer, offset)
        offset += 4
        offset += self.parse_metadata(buffer, offset)
        offset += self.parse_data(buffer, offset)

    def serialize(self, middle=b''):
        middle = struct.pack('>I', self.error_code)
        return Frame.serialize(self, middle)


class LeaseFrame(Frame):
    __slots__ = ('time_to_live', 'number_of_requests')

    def __init__(self):
        super().__init__(Type.LEASE)

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
    __slots__ = 'flags_respond'

    _FLAG_RESPOND_BIT = 0x2000

    def __init__(self):
        super().__init__(Type.KEEPALIVE)
        self.flags_respond = False

    # noinspection PyAttributeOutsideInit
    def parse(self, buffer, offset):
        offset += self.parse_header(buffer, offset)
        self.flags_respond = (self.flags & self._FLAG_RESPOND_BIT) != 0
        offset += self.parse_data(buffer, offset)

    def serialize(self, middle=b''):
        self.flags &= ~self._FLAG_RESPOND_BIT
        if self.flags_respond:
            self.flags |= self._FLAG_RESPOND_BIT
        return Frame.serialize(self, middle)


class RequestFrame(Frame):
    __slots__ = 'flags_follows'

    _FLAG_FOLLOWS_BIT = 0x2000

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


class RequestSubscriptionFrame(RequestFrame):
    __slots__ = 'initial_request_n'

    def __init__(self):
        super().__init__(Type.REQUEST_SUB)
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
    __slots__ = ('initial_request_n', 'flags_complete', 'flags_initial')

    _FLAG_COMPLETE_BIT = 0x1000
    _FLAG_INITIAL_BIT = 0x0800

    def __init__(self):
        super().__init__(Type.REQUEST_CHANNEL)
        self.flags_complete = False
        self.flags_initial = False

    # noinspection PyAttributeOutsideInit
    def parse(self, buffer, offset):
        offset += RequestFrame.parse(self, buffer, offset)
        self.flags_complete = (self.flags & self._FLAG_COMPLETE_BIT) != 0
        self.flags_initial = (self.flags & self._FLAG_INITIAL_BIT) != 0
        if self.flags_initial:
            self.initial_request_n, = struct.unpack_from('>I', buffer, offset)
            offset += 4
        offset += self.parse_metadata(buffer, offset)
        offset += self.parse_data(buffer, offset)


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

    # noinspection PyAttributeOutsideInit
    def parse(self, buffer, offset):
        offset += self.parse_header(buffer, offset)
        offset += self.parse_metadata(buffer, offset)


class ResponseFrame(Frame):
    __slots__ = ('flags_follows', 'flags_complete')

    _FLAG_FOLLOWS_BIT = 0x2000
    _FLAG_COMPLETE_BIT = 0x1000

    def __init__(self):
        super().__init__(Type.RESPONSE)
        self.flags_follows = False
        self.flags_complete = False

    # noinspection PyAttributeOutsideInit
    def parse(self, buffer, offset):
        offset += self.parse_header(buffer, offset)
        self.flags_follows = (self.flags & self._FLAG_FOLLOWS_BIT) != 0
        self.flags_complete = (self.flags & self._FLAG_COMPLETE_BIT) != 0
        offset += self.parse_metadata(buffer, offset)
        offset += self.parse_data(buffer, offset)

    def serialize(self, middle=b''):
        self.flags &= ~(self._FLAG_FOLLOWS_BIT | self._FLAG_COMPLETE_BIT)
        if self.flags_follows:
            self.flags |= self._FLAG_FOLLOWS_BIT
        if self.flags_complete:
            self.flags |= self._FLAG_COMPLETE_BIT
        return Frame.serialize(self)


class MetadataPushFrame(Frame):
    __slots__ = ()

    def __init__(self):
        super().__init__(Type.METADATA_PUSH)
        # noinspection PyDunderSlots
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
    Type.REQUEST_SUB: RequestSubscriptionFrame,
    Type.REQUEST_CHANNEL: RequestChannelFrame,
    Type.REQUEST_N: RequestNFrame,
    Type.CANCEL: CancelFrame,
    Type.RESPONSE: ResponseFrame,
    Type.ERROR: ErrorFrame,
    Type.METADATA_PUSH: MetadataPushFrame,
    # Type.RESUME: ResumeFrame,
    # Type.RESUME_OK: ResumeOKFrame,
}


def parse(buffer, offset):
    frame_type, = struct.unpack_from('>H', buffer, offset + 4)
    try:
        frame = _table[frame_type]()
        frame.parse(buffer, offset)
        return frame
    except KeyError:
        raise ParseError('Wrong frame type: {}'.format(frame_type))
