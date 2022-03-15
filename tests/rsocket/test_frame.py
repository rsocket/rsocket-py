import asyncstdlib
import pytest

from rsocket.error_codes import ErrorCode
from rsocket.exceptions import RSocketProtocolError
from rsocket.extensions.authentication_types import WellKnownAuthenticationTypes
from rsocket.extensions.composite_metadata import CompositeMetadata
from rsocket.extensions.mimetypes import WellKnownMimeTypes
from rsocket.frame import (SetupFrame, CancelFrame, ErrorFrame, FrameType,
                           RequestResponseFrame, RequestNFrame, ResumeFrame,
                           MetadataPushFrame, PayloadFrame, LeaseFrame, ResumeOKFrame, KeepAliveFrame,
                           serialize_with_frame_size_header, RequestStreamFrame, RequestChannelFrame, ParseError,
                           parse_or_ignore)
from tests.rsocket.helpers import data_bits, build_frame, bits


@pytest.mark.parametrize('metadata_flag, metadata, lease, data', (
        (0, b'', 0, b'\x01\x02\x03'),
        (1, b'\x04\x05\x06\x07\x08', 1, b'\x01\x02\x03'),
        (1, b'\x04\x05\x06\x07\x08', 1, b''),
        (1, b'\x04\x05\x06\x07\x08', 0, b''),
))
async def test_setup_readable(frame_parser, metadata_flag, metadata, lease, data):
    def variable_length():
        length = len(data)
        if metadata_flag != 0:
            length += len(metadata) + 3
        return length

    items = [
        bits(24, 53 + variable_length(), 'Frame size'),
        bits(1, 0, 'Padding'),
        bits(31, 0, 'Stream id'),
        bits(6, 1, 'Frame type'),
        # Flags
        bits(1, 0, 'Ignore'),
        bits(1, metadata_flag, 'Metadata'),
        bits(1, 0, 'Resume'),
        bits(1, lease, 'Lease'),
        bits(6, 0, 'Padding flags'),
        # Version
        bits(16, 1, 'Major version'),
        bits(16, 0, 'Minor version'),
        # Timeouts
        bits(1, 0, 'Padding'),
        bits(31, 123, 'Time Between KEEPALIVE Frames'),
        bits(1, 0, 'Padding'),
        bits(31, 456, 'Max Lifetime'),
        # Resume token - not present
        # Meta-data mime
        bits(8, 24, 'Metadata mime length'),
        data_bits(b'application/octet-stream'),
        # Data mime
        bits(8, 9, 'Data mime length'),
        data_bits(b'text/html'),
    ]

    if metadata_flag:
        items.extend((
            bits(24, len(metadata), 'Metadata length'),
            data_bits(metadata, 'Metadata')
        ))

    items.extend((data_bits(data, 'Payload')))

    frame_data = build_frame(*items)

    frames = await asyncstdlib.builtins.list(frame_parser.receive_data(frame_data))
    frame = frames[0]
    assert isinstance(frame, SetupFrame)
    assert serialize_with_frame_size_header(frame) == frame_data

    assert frame.metadata_encoding == b'application/octet-stream'
    assert frame.data_encoding == b'text/html'
    assert frame.keep_alive_milliseconds == 123
    assert frame.max_lifetime_milliseconds == 456
    assert frame.data == data
    assert frame.metadata == metadata
    assert frame.flags_resume is False
    assert frame.flags_lease == bool(lease)
    assert frame.flags_complete is False
    assert frame.flags_ignore is False
    assert frame.flags_follows is False
    assert frame.frame_type is FrameType.SETUP


@pytest.mark.parametrize('lease', (
        (0),
        (1)
))
async def test_setup_with_resume(frame_parser, lease):
    data = build_frame(
        bits(24, 84, 'Frame size'),
        bits(1, 0, 'Padding'),
        bits(31, 0, 'Stream id'),
        bits(6, 1, 'Frame type'),
        # Flags
        bits(1, 0, 'Ignore'),
        bits(1, 1, 'Metadata'),
        bits(1, 1, 'Resume'),
        bits(1, lease, 'Lease'),
        bits(6, 0, 'Padding flags'),
        # Version
        bits(16, 1, 'Major version'),
        bits(16, 0, 'Minor version'),
        # Timeouts
        bits(1, 0, 'Padding'),
        bits(31, 123, 'Time Between KEEPALIVE Frames'),
        bits(1, 0, 'Padding'),
        bits(31, 456, 'Max Lifetime'),
        # Resume token
        bits(16, 18, 'Resume token length'),
        data_bits(b'resume_token_value'),
        # Meta-data mime
        bits(8, 24, 'Metadata mime length'),
        data_bits(b'application/octet-stream'),
        # Data mime
        bits(8, 9, 'Data mime length'),
        data_bits(b'text/html'),
        # Metadata
        bits(24, 5, 'Metadata length'),
        data_bits(b'\x04\x05\x06\x07\x08'),
        # Payload
        data_bits(b'\x01\x02\x03'),
    )

    frames = await asyncstdlib.builtins.list(frame_parser.receive_data(data))
    frame = frames[0]
    assert isinstance(frame, SetupFrame)
    assert serialize_with_frame_size_header(frame) == data

    assert frame.metadata_encoding == b'application/octet-stream'
    assert frame.data_encoding == b'text/html'
    assert frame.keep_alive_milliseconds == 123
    assert frame.max_lifetime_milliseconds == 456
    assert frame.data == b'\x01\x02\x03'
    assert frame.metadata == b'\x04\x05\x06\x07\x08'
    assert frame.resume_identification_token == b'resume_token_value'
    assert frame.flags_resume
    assert frame.flags_lease == bool(lease)
    assert frame.flags_complete is False
    assert frame.flags_ignore is False
    assert frame.flags_follows is False


@pytest.mark.parametrize('follows', (
        0,
        1
))
async def test_request_stream_frame(frame_parser, follows):
    data = build_frame(
        bits(24, 32, 'Frame size'),
        bits(1, 0, 'Padding'),
        bits(31, 15, 'Stream id'),
        bits(6, FrameType.REQUEST_STREAM.value, 'Frame type'),
        # Flags
        bits(1, 0, 'Ignore'),
        bits(1, 1, 'Metadata'),
        bits(1, follows, 'Follows'),
        bits(7, 0, 'Empty flags'),
        bits(1, 0, 'Padding'),
        bits(31, 10, 'Initial request N'),
        # Metadata
        bits(24, 16, 'Metadata length'),
        # Composite metadata
        bits(1, 1, 'Well known metadata type'),
        bits(7, WellKnownMimeTypes.MESSAGE_RSOCKET_ROUTING.value.id, 'Mime ID'),
        bits(24, 12, 'Metadata length'),
        bits(8, 11, 'Tag Length'),
        data_bits(b'target.path'),

        # Payload
        data_bits(b'\x01\x02\x03'),
    )

    frames = await asyncstdlib.builtins.list(frame_parser.receive_data(data))
    frame = frames[0]
    assert isinstance(frame, RequestStreamFrame)
    assert serialize_with_frame_size_header(frame) == data

    assert frame.data == b'\x01\x02\x03'
    assert frame.flags_follows == bool(follows)
    assert frame.flags_complete is False
    assert frame.stream_id == 15
    assert frame.frame_type is FrameType.REQUEST_STREAM

    composite_metadata = CompositeMetadata()
    composite_metadata.parse(frame.metadata)

    assert composite_metadata.items[0].encoding == b'message/x.rsocket.routing.v0'
    assert composite_metadata.items[0].tags == [b'target.path']

    assert composite_metadata.serialize() == frame.metadata


@pytest.mark.parametrize('follows, complete', (
        (0, 1),
        (1, 1),
        (0, 0),
        (1, 0)
))
async def test_request_channel_frame(frame_parser, follows, complete):
    data = build_frame(
        bits(24, 32, 'Frame size'),
        bits(1, 0, 'Padding'),
        bits(31, 15, 'Stream id'),
        bits(6, FrameType.REQUEST_CHANNEL.value, 'Frame type'),
        # Flags
        bits(1, 0, 'Ignore'),
        bits(1, 1, 'Metadata'),
        bits(1, follows, 'Follows'),
        bits(1, complete, 'Complete'),
        bits(6, 0, 'Empty flags'),
        bits(1, 0, 'Padding'),
        bits(31, 10, 'Initial request N'),
        # Metadata
        bits(24, 16, 'Metadata length'),
        # Composite metadata
        bits(1, 1, 'Well known metadata type'),
        bits(7, WellKnownMimeTypes.MESSAGE_RSOCKET_ROUTING.value.id, 'Mime ID'),
        bits(24, 12, 'Metadata length'),
        bits(8, 11, 'Tag Length'),
        data_bits(b'target.path'),

        # Payload
        data_bits(b'\x01\x02\x03'),
    )

    frames = await asyncstdlib.builtins.list(frame_parser.receive_data(data))
    frame = frames[0]
    assert isinstance(frame, RequestChannelFrame)
    assert serialize_with_frame_size_header(frame) == data

    assert frame.data == b'\x01\x02\x03'
    assert frame.flags_follows == bool(follows)
    assert frame.flags_complete == bool(complete)
    assert frame.stream_id == 15
    assert frame.frame_type is FrameType.REQUEST_CHANNEL

    composite_metadata = CompositeMetadata()
    composite_metadata.parse(frame.metadata)

    assert composite_metadata.items[0].encoding == b'message/x.rsocket.routing.v0'
    assert composite_metadata.items[0].tags == [b'target.path']

    assert composite_metadata.serialize() == frame.metadata


def test_basic_composite_metadata_item():
    data = build_frame(

        bits(1, 1, 'Well known metadata type'),
        bits(7, WellKnownMimeTypes.TEXT_PLAIN.value.id, 'Mime ID'),
        bits(24, 9, 'Metadata length'),
        data_bits(b'some data')
    )

    composite_metadata = CompositeMetadata()
    composite_metadata.parse(data)

    assert composite_metadata.items[0].content == b'some data'

    serialized = composite_metadata.serialize()

    assert data == serialized


async def test_request_with_composite_metadata(frame_parser):
    data = build_frame(
        bits(24, 28, 'Frame size'),
        bits(1, 0, 'Padding'),
        bits(31, 17, 'Stream id'),
        bits(6, FrameType.REQUEST_RESPONSE.value, 'Frame type'),
        # Flags
        bits(1, 0, 'Ignore'),
        bits(1, 1, 'Metadata'),
        bits(1, 0, 'Follows'),
        bits(7, 0, 'Empty flags'),
        # Metadata
        bits(24, 16, 'Metadata length'),
        # Composite metadata
        bits(1, 1, 'Well known metadata type'),
        bits(7, WellKnownMimeTypes.MESSAGE_RSOCKET_ROUTING.value.id, 'Mime ID'),
        bits(24, 12, 'Metadata length'),
        bits(8, 11, 'Tag Length'),
        data_bits(b'target.path'),

        # Payload
        data_bits(b'\x01\x02\x03'),
    )

    frames = await asyncstdlib.builtins.list(frame_parser.receive_data(data))
    frame = frames[0]
    assert isinstance(frame, RequestResponseFrame)
    assert serialize_with_frame_size_header(frame) == data

    assert frame.data == b'\x01\x02\x03'
    assert not frame.flags_follows
    assert frame.stream_id == 17
    assert frame.frame_type is FrameType.REQUEST_RESPONSE

    composite_metadata = CompositeMetadata()
    composite_metadata.parse(frame.metadata)

    assert composite_metadata.items[0].encoding == b'message/x.rsocket.routing.v0'
    assert composite_metadata.items[0].tags == [b'target.path']

    assert composite_metadata.serialize() == frame.metadata


async def test_composite_metadata_multiple_items():
    data = build_frame(

        bits(1, 1, 'Well known metadata type'),
        bits(7, WellKnownMimeTypes.MESSAGE_RSOCKET_ROUTING.value.id, 'Mime ID'),
        bits(24, 12, 'Metadata length'),
        bits(8, 11, 'Tag Length'),
        data_bits(b'target.path'),

        bits(1, 1, 'Well known metadata type'),
        bits(7, WellKnownMimeTypes.MESSAGE_RSOCKET_MIMETYPE.value.id, 'Mime ID'),
        bits(24, 1, 'Metadata length'),
        bits(1, 1, 'Well known metadata type'),
        bits(7, WellKnownMimeTypes.TEXT_CSS.value.id, 'Mime ID'),

        bits(1, 1, 'Well known metadata type'),
        bits(7, WellKnownMimeTypes.MESSAGE_RSOCKET_MIMETYPE.value.id, 'Mime ID'),
        bits(24, 26, 'Metadata length'),
        bits(1, 0, 'Not well known metadata type'),
        bits(7, 24, 'Encoding length'),
        data_bits(b'some-custom-encoding/type'),

        bits(1, 1, 'Well known metadata type'),
        bits(7, WellKnownMimeTypes.MESSAGE_RSOCKET_AUTHENTICATION.value.id, 'Mime ID'),
        bits(24, 19, 'Metadata length'),
        bits(1, 1, 'Well known authentication type'),
        bits(7, WellKnownAuthenticationTypes.SIMPLE.value.id, 'Authentication ID'),
        bits(16, 8, 'Username length'),
        data_bits(b'username'),
        data_bits(b'password')
    )

    composite_metadata = CompositeMetadata()
    composite_metadata.parse(data)

    assert composite_metadata.items[0].encoding == b'message/x.rsocket.routing.v0'
    assert composite_metadata.items[0].tags == [b'target.path']

    assert composite_metadata.items[1].encoding == b'message/x.rsocket.mime-type.v0'
    assert composite_metadata.items[1].data_encoding == b'text/css'

    assert composite_metadata.items[2].encoding == b'message/x.rsocket.mime-type.v0'
    assert composite_metadata.items[2].data_encoding == b'some-custom-encoding/type'

    assert composite_metadata.items[3].encoding == b'message/x.rsocket.authentication.v0'
    assert composite_metadata.items[3].authentication.username == b'username'
    assert composite_metadata.items[3].authentication.password == b'password'

    assert composite_metadata.serialize() == data


async def test_cancel(frame_parser):
    data = build_frame(
        bits(24, 6, 'Frame size'),
        bits(1, 0, 'Padding'),
        bits(31, 0, 'Stream id'),
        bits(6, 9, 'Frame type'),
        # Flags
        bits(1, 0, 'Ignore'),
        bits(1, 0, 'Metadata'),
        bits(8, 0, 'Padding flags'),
    )

    frames = await asyncstdlib.builtins.list(frame_parser.receive_data(data))
    frame = frames[0]
    assert isinstance(frame, CancelFrame)
    assert frame.frame_type is FrameType.CANCEL
    assert serialize_with_frame_size_header(frame) == data


async def test_error(frame_parser):
    data = build_frame(
        bits(24, 20, 'Frame size'),
        bits(1, 0, 'Padding'),
        bits(31, 0, 'Stream id'),
        bits(6, FrameType.ERROR.value, 'Frame type'),
        # Flags
        bits(1, 0, 'Ignore'),
        bits(1, 0, 'Metadata'),
        bits(8, 0, 'Padding flags'),
        # Request N
        bits(32, ErrorCode.REJECTED_SETUP.value, 'Number of frames to request'),
        data_bits(b'error data')
    )

    frames = await asyncstdlib.builtins.list(frame_parser.receive_data(data))
    frame = frames[0]
    assert isinstance(frame, ErrorFrame)
    assert serialize_with_frame_size_header(frame) == data

    assert frame.error_code == ErrorCode.REJECTED_SETUP
    assert frame.data == b'error data'
    assert frame.frame_type is FrameType.ERROR


async def test_request_n_frame(frame_parser):
    data = build_frame(
        bits(24, 10, 'Frame size'),
        bits(1, 0, 'Padding'),
        bits(31, 0, 'Stream id'),
        bits(6, 8, 'Frame type'),
        # Flags
        bits(1, 0, 'Ignore'),
        bits(1, 0, 'Metadata'),
        bits(8, 0, 'Padding flags'),
        # Request N
        bits(1, 0, 'Padding'),
        bits(31, 23, 'Number of frames to request'),
    )

    frames = await asyncstdlib.builtins.list(frame_parser.receive_data(data))
    frame = frames[0]
    assert isinstance(frame, RequestNFrame)
    assert serialize_with_frame_size_header(frame) == data

    assert frame.request_n == 23
    assert frame.frame_type is FrameType.REQUEST_N


async def test_resume_frame(frame_parser):
    data = build_frame(
        bits(24, 40, 'Frame size'),
        bits(1, 0, 'Padding'),
        bits(31, 0, 'Stream id'),
        bits(6, FrameType.RESUME, 'Frame type'),
        # Flags
        bits(1, 0, 'Ignore'),
        bits(1, 0, 'Metadata'),
        bits(8, 0, 'Padding flags'),
        # Version
        bits(16, 1, 'Major version'),
        bits(16, 0, 'Minor version'),
        # Resume token
        bits(16, 12, 'Token length'),
        data_bits(b'resume_token'),
        bits(1, 0, 'placeholder'),
        bits(63, 123, 'last server position'),
        bits(1, 0, 'placeholder'),
        bits(63, 456, 'first client position'),
    )

    frames = await asyncstdlib.builtins.list(frame_parser.receive_data(data))
    frame = frames[0]
    assert isinstance(frame, ResumeFrame)
    assert frame.last_server_position == 123
    assert frame.first_client_position == 456
    assert frame.frame_type is FrameType.RESUME
    assert serialize_with_frame_size_header(frame) == data


async def test_metadata_push_frame(frame_parser):
    data = build_frame(
        bits(24, 14, 'Frame size'),
        bits(1, 0, 'Padding'),
        bits(31, 0, 'Stream id'),
        bits(6, FrameType.METADATA_PUSH, 'Frame type'),
        # Flags
        bits(1, 0, 'Ignore'),
        bits(1, 1, 'Metadata'),
        bits(8, 0, 'Padding flags'),
        data_bits(b'metadata')
    )

    frames = await asyncstdlib.builtins.list(frame_parser.receive_data(data))
    frame = frames[0]
    assert isinstance(frame, MetadataPushFrame)
    assert frame.metadata == b'metadata'
    assert frame.frame_type is FrameType.METADATA_PUSH
    assert serialize_with_frame_size_header(frame) == data


async def test_payload_frame(frame_parser):
    data = build_frame(
        bits(24, 28, 'Frame size'),
        bits(1, 0, 'Padding'),
        bits(31, 6, 'Stream id'),
        bits(6, FrameType.PAYLOAD, 'Frame type'),
        # Flags
        bits(1, 0, 'Ignore'),
        bits(1, 1, 'Metadata'),
        bits(8, 0, 'Padding flags'),
        bits(24, 8, 'Metadata length'),
        data_bits(b'metadata'),
        data_bits(b'actual_data'),
    )

    frames = await asyncstdlib.builtins.list(frame_parser.receive_data(data))
    frame = frames[0]
    assert isinstance(frame, PayloadFrame)
    assert frame.metadata == b'metadata'
    assert frame.data == b'actual_data'
    assert frame.frame_type is FrameType.PAYLOAD
    assert frame.stream_id == 6
    assert serialize_with_frame_size_header(frame) == data


async def test_lease_frame(frame_parser):
    data = build_frame(
        bits(24, 37, 'Frame size'),
        bits(1, 0, 'Padding'),
        bits(31, 0, 'Stream id'),
        bits(6, FrameType.LEASE, 'Frame type'),
        # Flags
        bits(1, 0, 'Ignore'),
        bits(1, 1, 'Metadata'),
        bits(8, 0, 'Padding flags'),
        bits(1, 0, 'Padding'),
        bits(31, 456, 'Time to live'),
        bits(1, 0, 'Padding'),
        bits(31, 123, 'Number of requests'),
        data_bits(b'Metadata on lease frame')
    )

    frames = await asyncstdlib.builtins.list(frame_parser.receive_data(data))
    frame = frames[0]
    assert isinstance(frame, LeaseFrame)
    assert frame.number_of_requests == 123
    assert frame.time_to_live == 456
    assert frame.frame_type is FrameType.LEASE
    assert frame.metadata == b'Metadata on lease frame'
    assert serialize_with_frame_size_header(frame) == data


async def test_resume_ok_frame(frame_parser):
    data = build_frame(
        bits(24, 14, 'Frame size'),
        bits(1, 0, 'Padding'),
        bits(31, 0, 'Stream id'),
        bits(6, FrameType.RESUME_OK, 'Frame type'),
        # Flags
        bits(1, 0, 'Ignore'),
        bits(1, 0, 'Metadata'),
        bits(8, 0, 'Padding flags'),
        bits(1, 0, 'Padding'),
        bits(63, 456, 'Last Received Client Position')
    )

    frames = await asyncstdlib.builtins.list(frame_parser.receive_data(data))
    frame = frames[0]
    assert isinstance(frame, ResumeOKFrame)
    assert frame.last_received_client_position == 456
    assert frame.frame_type is FrameType.RESUME_OK
    assert serialize_with_frame_size_header(frame) == data


async def test_keepalive_frame(frame_parser):
    data = build_frame(
        bits(24, 29, 'Frame size'),
        bits(1, 0, 'Padding'),
        bits(31, 0, 'Stream id'),
        bits(6, FrameType.KEEPALIVE, 'Frame type'),
        # Flags
        bits(1, 0, 'Ignore'),
        bits(1, 0, 'Metadata'),
        bits(8, 0, 'Padding flags'),
        bits(1, 0, 'Padding'),
        bits(63, 456, 'Last Received Client Position'),
        data_bits(b'additional data')
    )

    frames = await asyncstdlib.builtins.list(frame_parser.receive_data(data))
    frame = frames[0]

    assert isinstance(frame, KeepAliveFrame)
    assert frame.last_received_position == 456
    assert frame.stream_id == 0
    assert frame.frame_type is FrameType.KEEPALIVE
    assert frame.data == b'additional data'
    assert serialize_with_frame_size_header(frame) == data


def test_parse_error_on_frame_too_short():
    with pytest.raises(ParseError):
        parse_or_ignore(b'1')


def test_parse_broken_frame_raises_exception():
    broken_frame_data = build_frame(
        bits(1, 0, 'Padding'),
        bits(31, 0, 'Stream id'),
        bits(6, 8, 'Frame type'),
        # Flags
        bits(1, 0, 'Ignore'),
        bits(1, 0, 'Metadata'),
        bits(8, 0, 'Padding flags'),
        # Request N
        bits(1, 0, 'Padding'),
        bits(13, 23, 'Number of frames to request - broken. smaller than 31 bits'),
    )

    with pytest.raises(RSocketProtocolError):
        parse_or_ignore(broken_frame_data)
