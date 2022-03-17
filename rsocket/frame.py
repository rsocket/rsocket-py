import abc
import struct
from abc import ABCMeta
from enum import IntEnum, unique
from typing import Tuple, Optional

from rsocket.error_codes import ErrorCode
from rsocket.exceptions import RSocketProtocolError, ParseError, RSocketUnknownFrameType
from rsocket.frame_helpers import is_flag_set, unpack_position, pack_position, unpack_24bit, pack_24bit, unpack_32bit, \
    ensure_bytes

PROTOCOL_MAJOR_VERSION = 1
PROTOCOL_MINOR_VERSION = 0

MASK_31_BITS = 0x7FFFFFFF
CONNECTION_STREAM_ID = 0
MAX_REQUEST_N = 0x7FFFFFFF

_FLAG_METADATA_BIT = 0x100
_FLAG_IGNORE_BIT = 0x200
_FLAG_COMPLETE_BIT = 0x40
_FLAG_FOLLOWS_BIT = 0x80
_FLAG_LEASE_BIT = 0x40
_FLAG_RESUME_BIT = 0x80
_FLAG_RESPOND_BIT = 0x80
_FLAG_NEXT_BIT = 0x20


@unique
class FrameType(IntEnum):
    SETUP = 1
    LEASE = 2
    KEEPALIVE = 3
    REQUEST_RESPONSE = 4
    REQUEST_FNF = 5
    REQUEST_STREAM = 6
    REQUEST_CHANNEL = 7
    REQUEST_N = 8
    CANCEL = 9
    PAYLOAD = 10
    ERROR = 11
    METADATA_PUSH = 12
    RESUME = 13
    RESUME_OK = 14
    EXT = 0xFFFF


HEADER_LENGTH = 6  # A full header is 4 (stream) + 2 (type, flags) bytes.


class Header:
    __slots__ = (
        'length',
        'frame_type',
        'stream_id',
        'flags_ignore',
        'flags_metadata'
    )


def parse_header(frame: Header, buffer: bytes, offset: int) -> int:
    frame.length = len(buffer)
    frame.stream_id, frame.frame_type, flags = struct.unpack_from('>IBB', buffer, offset)
    flags |= (frame.frame_type & 3) << 8
    frame_type_id = frame.frame_type >> 2
    frame.frame_type = FrameType(frame_type_id)
    frame.flags_ignore = is_flag_set(flags, _FLAG_IGNORE_BIT)
    frame.flags_metadata = is_flag_set(flags, _FLAG_METADATA_BIT)
    return flags


class FragmentableFrame:
    __slots__ = (
        'metadata',
        'data',
        'flags_follows',
        'flags_complete',
        'flags_next',
        'stream_id'
    )


class Frame(Header, metaclass=ABCMeta):
    __slots__ = (
        'metadata',
        'data',
        'flags_follows',
        'flags_complete',
        'metadata_only'
    )

    def __init__(self, frame_type: FrameType):
        self.length = 0
        self.frame_type = frame_type
        self.stream_id = CONNECTION_STREAM_ID
        self.metadata = b''
        self.data = b''

        self.flags_ignore = False
        self.flags_metadata = False
        self.flags_follows = False
        self.flags_complete = False

        self.metadata_only = False

    def parse_metadata(self, buffer: bytes, offset: int) -> int:
        if not self.flags_metadata:
            return 0

        if not self.metadata_only:
            length = unpack_24bit(buffer, offset)
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

    @abc.abstractmethod
    def parse(self, buffer: bytes, offset: int):
        ...

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

        struct.pack_into('>I', buffer, offset, self.stream_id)
        offset += 4

        buffer[offset] = (self.frame_type << 2) | (flags >> 8)
        offset += 1
        buffer[offset] = flags & 0xff
        offset += 1

        buffer[offset:offset + len(middle)] = middle[:]
        offset += len(middle)

        if self.flags_metadata and self.metadata:
            length = len(self.metadata)
            if not self.metadata_only:
                buffer[offset:offset + 3] = pack_24bit(length)
                offset += 3
            buffer[offset:offset + length] = self.metadata[:]
            offset += length

        if not self.metadata_only and self.data:
            buffer[offset:offset + len(self.data)] = self.data[:]
            offset += len(self.data)

        return bytes(buffer)

    def _compute_frame_length(self, middle: bytes) -> int:
        header_length = HEADER_LENGTH
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
        super().__init__(FrameType.SETUP)
        self.major_version = PROTOCOL_MAJOR_VERSION
        self.minor_version = PROTOCOL_MINOR_VERSION
        self.flags_lease = False
        self.flags_resume = False

    # noinspection PyAttributeOutsideInit
    def parse(self, buffer: bytes, offset: int):
        flags = parse_header(self, buffer, offset)
        offset += HEADER_LENGTH
        self.flags_lease = is_flag_set(flags, _FLAG_LEASE_BIT)
        self.flags_resume = is_flag_set(flags, _FLAG_RESUME_BIT)

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
            length = struct.unpack_from('b', buffer, offset)[0]
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


class InvalidFrame:
    pass


class ErrorFrame(Frame):
    __slots__ = 'error_code'

    def __init__(self):
        super().__init__(FrameType.ERROR)

    # noinspection PyAttributeOutsideInit
    def parse(self, buffer: bytes, offset: int):
        parse_header(self, buffer, offset)
        offset += HEADER_LENGTH
        self.error_code = ErrorCode(unpack_32bit(buffer, offset))
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
        super().__init__(FrameType.LEASE)
        self.metadata_only = True
        self.time_to_live = None
        self.number_of_requests = None

    def parse(self, buffer: bytes, offset: int):
        parse_header(self, buffer, offset)
        offset += HEADER_LENGTH
        time_to_live, number_of_requests = struct.unpack_from('>II', buffer, offset)
        self.time_to_live = time_to_live & MASK_31_BITS
        self.number_of_requests = number_of_requests & MASK_31_BITS
        offset += self.parse_metadata(buffer, offset + 8)

    def serialize(self, middle=b'', flags=0):
        middle = struct.pack('>II',
                             self.time_to_live & MASK_31_BITS,
                             self.number_of_requests & MASK_31_BITS)
        return Frame.serialize(self, middle, flags)


class KeepAliveFrame(Frame):
    __slots__ = ('flags_respond', 'last_received_position')

    def __init__(self, data=b'', metadata=b''):
        super().__init__(FrameType.KEEPALIVE)
        self.stream_id = CONNECTION_STREAM_ID
        self.flags_respond = False
        self.data = data
        self.metadata = metadata
        self.last_received_position = 0

    def parse(self, buffer: bytes, offset: int):
        flags = parse_header(self, buffer, offset)
        offset += HEADER_LENGTH
        self.flags_respond = is_flag_set(flags, _FLAG_RESPOND_BIT)
        self.last_received_position = unpack_position(buffer[offset:offset + 8])
        offset += 8
        offset += self.parse_data(buffer, offset)

    def serialize(self, middle=b'', flags: int = 0) -> bytes:
        flags &= ~_FLAG_RESPOND_BIT
        if self.flags_respond:
            flags |= _FLAG_RESPOND_BIT
        middle += pack_position(self.last_received_position)
        return Frame.serialize(self, middle, flags)


class RequestFrame(Frame):
    __slots__ = (
        'flags_follows',
    )

    def __init__(self, frame_type):
        super().__init__(frame_type)
        self.flags_follows = False

    def parse(self, buffer, offset: int) -> Tuple[int, int]:
        flags = parse_header(self, buffer, offset)
        self.flags_follows = is_flag_set(flags, _FLAG_FOLLOWS_BIT)
        return HEADER_LENGTH, flags

    def serialize(self, middle=b'', flags: int = 0) -> bytes:
        flags &= ~_FLAG_FOLLOWS_BIT

        if self.flags_follows:
            flags |= _FLAG_FOLLOWS_BIT

        return Frame.serialize(self, middle, flags)

    def _parse_payload(self, buffer: bytes, offset: int):
        offset += self.parse_metadata(buffer, offset)
        offset += self.parse_data(buffer, offset)


class RequestResponseFrame(RequestFrame):
    __slots__ = ()

    def __init__(self):
        super().__init__(FrameType.REQUEST_RESPONSE)

    # noinspection PyAttributeOutsideInit
    def parse(self, buffer, offset):
        header = RequestFrame.parse(self, buffer, offset)
        offset += header[0]
        self._parse_payload(buffer, offset)


class RequestFireAndForgetFrame(RequestFrame):
    __slots__ = ()

    def __init__(self):
        super().__init__(FrameType.REQUEST_FNF)

    def parse(self, buffer, offset):
        header = RequestFrame.parse(self, buffer, offset)
        offset += header[0]
        self._parse_payload(buffer, offset)


class RequestStreamFrame(RequestFrame):
    __slots__ = 'initial_request_n'

    def __init__(self):
        super().__init__(FrameType.REQUEST_STREAM)
        self.initial_request_n = 0

    def parse(self, buffer, offset):
        header = RequestFrame.parse(self, buffer, offset)
        offset += header[0]
        self.initial_request_n = unpack_32bit(buffer, offset)
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
    )

    def __init__(self):
        super().__init__(FrameType.REQUEST_CHANNEL)
        self.flags_complete = False

    # noinspection PyAttributeOutsideInit
    def parse(self, buffer, offset):
        header = RequestFrame.parse(self, buffer, offset)
        offset += header[0]
        flags = header[1]
        self.flags_complete = is_flag_set(flags, _FLAG_COMPLETE_BIT)
        self.flags_follows = is_flag_set(flags, _FLAG_FOLLOWS_BIT)
        self.initial_request_n = unpack_32bit(buffer, offset)
        offset += 4
        self._parse_payload(buffer, offset)

    def serialize(self, middle=b'', flags=0):
        middle = struct.pack('>I', self.initial_request_n)

        flags &= ~_FLAG_COMPLETE_BIT
        if self.flags_complete:
            flags |= _FLAG_COMPLETE_BIT

        return RequestFrame.serialize(self, middle, flags)


class RequestNFrame(RequestFrame):
    __slots__ = 'request_n'

    def __init__(self):
        super().__init__(FrameType.REQUEST_N)

    # noinspection PyAttributeOutsideInit
    def parse(self, buffer, offset):
        parse_header(self, buffer, offset)
        offset += HEADER_LENGTH
        self.request_n = unpack_32bit(buffer, offset)

    def serialize(self, middle=b'', flags=0):
        middle = struct.pack('>I', self.request_n)
        return Frame.serialize(self, middle, flags)


class CancelFrame(Frame):

    def __init__(self):
        super().__init__(FrameType.CANCEL)

    def parse(self, buffer, offset):
        parse_header(self, buffer, offset)


class PayloadFrame(Frame):
    __slots__ = (
        'flags_follows',
        'flags_complete',
        'flags_next'
    )

    def __init__(self):
        super().__init__(FrameType.PAYLOAD)
        self.flags_follows = False
        self.flags_complete = False
        self.flags_next = False

    def parse(self, buffer, offset):
        flags = parse_header(self, buffer, offset)
        offset += HEADER_LENGTH
        self.flags_follows = is_flag_set(flags, _FLAG_FOLLOWS_BIT)
        self.flags_complete = is_flag_set(flags, _FLAG_COMPLETE_BIT)
        self.flags_next = is_flag_set(flags, _FLAG_NEXT_BIT)
        offset += self.parse_metadata(buffer, offset)
        offset += self.parse_data(buffer, offset)

    def serialize(self, middle=b'', flags=0):
        flags &= ~(_FLAG_FOLLOWS_BIT | _FLAG_COMPLETE_BIT |
                   _FLAG_NEXT_BIT)
        if self.flags_follows:
            flags |= _FLAG_FOLLOWS_BIT
        if self.flags_complete:
            flags |= _FLAG_COMPLETE_BIT
        if self.flags_next:
            flags |= _FLAG_NEXT_BIT
        return Frame.serialize(self, flags=flags)


class MetadataPushFrame(Frame):
    __slots__ = ()

    def __init__(self):
        super().__init__(FrameType.METADATA_PUSH)
        self.stream_id = CONNECTION_STREAM_ID
        self.metadata_only = True
        self.flags_metadata = True

    def parse(self, buffer, offset):
        parse_header(self, buffer, offset)
        offset += HEADER_LENGTH
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
        super().__init__(FrameType.RESUME)
        self.major_version = PROTOCOL_MAJOR_VERSION
        self.minor_version = PROTOCOL_MINOR_VERSION

    # noinspection PyAttributeOutsideInit
    def parse(self, buffer: bytes, offset: int):
        parse_header(self, buffer, offset)
        offset += HEADER_LENGTH

        (self.major_version, self.minor_version) = (
            struct.unpack_from('>HH', buffer, offset))

        offset += 4

        self.token_length = struct.unpack_from('>H', buffer, offset)[0]
        offset += 2
        self.resume_identification_token = (
            buffer[offset:offset + self.token_length])
        offset += self.token_length

        self.last_server_position = unpack_position(buffer[offset:offset + 8])
        offset += 8
        self.first_client_position = unpack_position(buffer[offset:])

    def serialize(self, middle=b'', flags=0) -> bytes:
        flags &= ~(_FLAG_LEASE_BIT | _FLAG_RESUME_BIT)

        middle = struct.pack('>HH', self.major_version, self.minor_version)
        middle += struct.pack('>H', len(self.resume_identification_token))
        # assert len(self.resume_identification_token) == self.token_length
        # assert isinstance(self.resume_identification_token, bytes)
        middle += self.resume_identification_token

        middle += pack_position(self.last_server_position)
        middle += pack_position(self.first_client_position)

        return Frame.serialize(self, middle)


class ResumeOKFrame(Frame):
    __slots__ = (
        'last_received_client_position'
    )

    def __init__(self):
        super().__init__(FrameType.RESUME_OK)
        self.last_received_client_position = 0

    def parse(self, buffer: bytes, offset: int):
        parse_header(self, buffer, offset)
        offset += HEADER_LENGTH
        self.last_received_client_position = unpack_position(buffer[offset:offset + 8])

    def serialize(self, middle=b'', flags: int = 0) -> bytes:
        serialized = pack_position(self.last_received_client_position)
        return super().serialize(serialized)


class ExtendedFrame(Frame, metaclass=abc.ABCMeta):
    __slots__ = (
        'extended_type'
    )

    def __init__(self):
        super().__init__(FrameType.EXT)

    @abc.abstractmethod
    def serialize(self, middle=b'', flags: int = 0) -> bytes:
        ...


_frame_class_by_id = {
    FrameType.SETUP: SetupFrame,
    FrameType.LEASE: LeaseFrame,
    FrameType.KEEPALIVE: KeepAliveFrame,
    FrameType.REQUEST_RESPONSE: RequestResponseFrame,
    FrameType.REQUEST_FNF: RequestFireAndForgetFrame,
    FrameType.REQUEST_STREAM: RequestStreamFrame,
    FrameType.REQUEST_CHANNEL: RequestChannelFrame,
    FrameType.REQUEST_N: RequestNFrame,
    FrameType.CANCEL: CancelFrame,
    FrameType.PAYLOAD: PayloadFrame,
    FrameType.ERROR: ErrorFrame,
    FrameType.METADATA_PUSH: MetadataPushFrame,
    FrameType.RESUME: ResumeFrame,
    FrameType.RESUME_OK: ResumeOKFrame,
}


def parse_or_ignore(buffer: bytes) -> Optional[Frame]:
    if len(buffer) < HEADER_LENGTH:
        raise ParseError('Frame too short: {} bytes'.format(len(buffer)))

    header = Header()
    parse_header(header, buffer, 0)

    try:
        frame = _frame_class_by_id[header.frame_type]()
    except KeyError as exception:
        raise RSocketUnknownFrameType(header.frame_type) from exception

    try:
        frame.parse(buffer, 0)
        return frame
    except Exception as exception:
        if not header.flags_ignore:
            raise RSocketProtocolError(ErrorCode.CONNECTION_ERROR, str(exception)) from exception


def is_fragmentable_frame(frame: Frame) -> bool:
    return isinstance(frame, (
        PayloadFrame,
        RequestResponseFrame,
        RequestChannelFrame,
        RequestStreamFrame,
        RequestFireAndForgetFrame
    ))


def exception_to_error_frame(stream_id: int, exception: Exception) -> ErrorFrame:
    frame = ErrorFrame()
    frame.stream_id = stream_id

    if isinstance(exception, RSocketProtocolError):
        frame.error_code = exception.error_code
        frame.data = ensure_bytes(exception.data)
    else:
        frame.error_code = ErrorCode.APPLICATION_ERROR
        frame.data = str(exception).encode()

    return frame


def error_frame_to_exception(frame: ErrorFrame) -> Exception:
    if frame.error_code != ErrorCode.APPLICATION_ERROR:
        return RSocketProtocolError(frame.error_code, data=frame.data.decode())

    return RuntimeError(frame.data.decode('utf-8'))


def serialize_with_frame_size_header(frame: Frame) -> bytes:
    serialized_frame = frame.serialize()
    header = struct.pack('>I', len(serialized_frame))[1:]
    full_frame = header + serialized_frame
    return full_frame


initiate_request_frame_types = (RequestResponseFrame,
                                RequestStreamFrame,
                                RequestChannelFrame,
                                RequestFireAndForgetFrame
                                )
