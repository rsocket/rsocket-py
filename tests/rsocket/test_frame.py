import asyncstdlib
import pytest

from rsocket.extensions.authentication_types import WellKnownAuthenticationTypes
from rsocket.extensions.composite_metadata import CompositeMetadata
from rsocket.extensions.mimetypes import WellKnownMimeTypes
from rsocket.frame import (SetupFrame, CancelFrame, ErrorFrame, Type,
                           RequestResponseFrame, RequestNFrame, ResumeFrame,
                           MetadataPushFrame, PayloadFrame, LeaseFrame)
from tests.rsocket.helpers import data_bits, build_frame, bits


@pytest.mark.asyncio
async def test_setup(connection):
    data = b'\x00\x00\x40\x00\x00\x00\x00\x05\x00\x00\x01\x00\x00\x00\x00\x00'
    data += b'\x7b\x00\x00\x01\xc8\x18\x61\x70\x70\x6c\x69\x63\x61\x74\x69\x6f'
    data += b'\x6e\x2f\x6f\x63\x74\x65\x74\x2d\x73\x74\x72\x65\x61\x6d\x09\x74'
    data += b'\x65\x78\x74\x2f\x68\x74\x6d\x6c\x00\x00\x05\x04\x05\x06\x07\x08'
    data += b'\x01\x02\x03'

    frames = await asyncstdlib.builtins.list(connection.receive_data(data))
    frame = frames[0]
    assert isinstance(frame, SetupFrame)
    assert frame.serialize() == data

    assert frame.metadata_encoding == b'application/octet-stream'
    assert frame.data_encoding == b'text/html'
    assert frame.keep_alive_milliseconds == 123
    assert frame.max_lifetime_milliseconds == 456
    assert frame.data == b'\x01\x02\x03'
    assert frame.metadata == b'\x04\x05\x06\x07\x08'


@pytest.mark.asyncio
async def test_setup_readable(connection):
    data = build_frame(
        bits(24, 64, 'Frame size'),
        bits(1, 0, 'Padding'),
        bits(31, 0, 'Stream id'),
        bits(6, 1, 'Frame type'),
        # Flags
        bits(1, 0, 'Ignore'),
        bits(1, 1, 'Metadata'),
        bits(1, 0, 'Resume'),
        bits(1, 0, 'Lease'),
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
        # Metadata
        bits(24, 5, 'Metadata length'),
        data_bits(b'\x04\x05\x06\x07\x08'),
        # Payload
        data_bits(b'\x01\x02\x03'),
    )

    frames = await asyncstdlib.builtins.list(connection.receive_data(data))
    frame = frames[0]
    assert isinstance(frame, SetupFrame)
    assert frame.serialize() == data

    assert frame.metadata_encoding == b'application/octet-stream'
    assert frame.data_encoding == b'text/html'
    assert frame.keep_alive_milliseconds == 123
    assert frame.max_lifetime_milliseconds == 456
    assert frame.data == b'\x01\x02\x03'
    assert frame.metadata == b'\x04\x05\x06\x07\x08'
    assert not frame.flags_resume


@pytest.mark.asyncio
async def test_setup_with_resume(connection):
    data = build_frame(
        bits(24, 84, 'Frame size'),
        bits(1, 0, 'Padding'),
        bits(31, 0, 'Stream id'),
        bits(6, 1, 'Frame type'),
        # Flags
        bits(1, 0, 'Ignore'),
        bits(1, 1, 'Metadata'),
        bits(1, 1, 'Resume'),
        bits(1, 0, 'Lease'),
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

    frames = await asyncstdlib.builtins.list(connection.receive_data(data))
    frame = frames[0]
    assert isinstance(frame, SetupFrame)
    assert frame.serialize() == data

    assert frame.metadata_encoding == b'application/octet-stream'
    assert frame.data_encoding == b'text/html'
    assert frame.keep_alive_milliseconds == 123
    assert frame.max_lifetime_milliseconds == 456
    assert frame.data == b'\x01\x02\x03'
    assert frame.metadata == b'\x04\x05\x06\x07\x08'
    assert frame.resume_identification_token == b'resume_token_value'
    assert frame.flags_resume


@pytest.mark.asyncio
async def test_request_with_composite_metadata(connection):
    data = build_frame(
        bits(24, 28, 'Frame size'),
        bits(1, 0, 'Padding'),
        bits(31, 0, 'Stream id'),
        bits(6, Type.REQUEST_RESPONSE.value, 'Frame type'),
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

    frames = await asyncstdlib.builtins.list(connection.receive_data(data))
    frame = frames[0]
    assert isinstance(frame, RequestResponseFrame)
    assert frame.serialize() == data

    assert frame.data == b'\x01\x02\x03'

    composite_metadata = CompositeMetadata()
    composite_metadata.parse(frame.metadata)

    assert composite_metadata.items[0].encoding == b'message/x.rsocket.routing.v0'
    assert composite_metadata.items[0].tags == [b'target.path']

    assert composite_metadata.serialize() == frame.metadata


@pytest.mark.asyncio
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
        bits(7, 25, 'Encoding length'),
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


@pytest.mark.asyncio
async def test_cancel(connection):
    data = b'\x00\x00\x06\x00\x00\x00\x7b\x24\x00'
    frames = await asyncstdlib.builtins.list(connection.receive_data(data))
    frame = frames[0]
    assert isinstance(frame, CancelFrame)
    assert frame.serialize() == data


@pytest.mark.asyncio
async def test_error(connection):
    data = b'\x00\x00\x13\x00\x00\x26\x6a\x2c\x00\x00\x00\x02\x04\x77\x65\x69'
    data += b'\x72\x64\x6e\x65\x73\x73'
    frames = await asyncstdlib.builtins.list(connection.receive_data(data))
    frame = frames[0]
    assert isinstance(frame, ErrorFrame)
    assert frame.serialize() == data


@pytest.mark.asyncio
async def test_multiple_frames(connection):
    data = b'\x00\x00\x06\x00\x00\x00\x7b\x24\x00'
    data += b'\x00\x00\x13\x00\x00\x26\x6a\x2c\x00\x00\x00\x02\x04\x77\x65\x69'
    data += b'\x72\x64\x6e\x65\x73\x73'
    data += b'\x00\x00\x06\x00\x00\x00\x7b\x24\x00'
    data += b'\x00\x00\x06\x00\x00\x00\x7b\x24\x00'
    data += b'\x00\x00\x06\x00\x00\x00\x7b\x24\x00'
    frames = await asyncstdlib.builtins.list(connection.receive_data(data))
    assert len(frames) == 5


@pytest.mark.asyncio
async def test_request_n_frame(connection):
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

    frames = await asyncstdlib.builtins.list(connection.receive_data(data))
    frame = frames[0]
    assert isinstance(frame, RequestNFrame)
    assert frame.serialize() == data

    assert frame.request_n == 23


@pytest.mark.asyncio
async def test_resume_frame(connection):
    data = build_frame(
        bits(24, 40, 'Frame size'),
        bits(1, 0, 'Padding'),
        bits(31, 0, 'Stream id'),
        bits(6, Type.RESUME, 'Frame type'),
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

    frames = await asyncstdlib.builtins.list(connection.receive_data(data))
    frame = frames[0]
    assert isinstance(frame, ResumeFrame)
    assert frame.last_server_position == 123
    assert frame.first_client_position == 456
    assert frame.serialize() == data


@pytest.mark.asyncio
async def test_metadata_push_frame(connection):
    data = build_frame(
        bits(24, 14, 'Frame size'),
        bits(1, 0, 'Padding'),
        bits(31, 0, 'Stream id'),
        bits(6, Type.METADATA_PUSH, 'Frame type'),
        # Flags
        bits(1, 0, 'Ignore'),
        bits(1, 1, 'Metadata'),
        bits(8, 0, 'Padding flags'),
        data_bits(b'metadata')
    )

    frames = await asyncstdlib.builtins.list(connection.receive_data(data))
    frame = frames[0]
    assert isinstance(frame, MetadataPushFrame)
    assert frame.metadata == b'metadata'
    assert frame.serialize() == data


@pytest.mark.asyncio
async def test_payload_frame(connection):
    data = build_frame(
        bits(24, 28, 'Frame size'),
        bits(1, 0, 'Padding'),
        bits(31, 0, 'Stream id'),
        bits(6, Type.PAYLOAD, 'Frame type'),
        # Flags
        bits(1, 0, 'Ignore'),
        bits(1, 1, 'Metadata'),
        bits(8, 0, 'Padding flags'),
        bits(24, 8, 'Metadata length'),
        data_bits(b'metadata'),
        data_bits(b'actual_data'),
    )

    frames = await asyncstdlib.builtins.list(connection.receive_data(data))
    frame = frames[0]
    assert isinstance(frame, PayloadFrame)
    assert frame.metadata == b'metadata'
    assert frame.data == b'actual_data'
    assert frame.serialize() == data


@pytest.mark.asyncio
async def test_lease_frame(connection):
    data = build_frame(
        bits(24, 37, 'Frame size'),
        bits(1, 0, 'Padding'),
        bits(31, 0, 'Stream id'),
        bits(6, Type.LEASE, 'Frame type'),
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

    frames = await asyncstdlib.builtins.list(connection.receive_data(data))
    frame = frames[0]
    assert isinstance(frame, LeaseFrame)
    assert frame.number_of_requests == 123
    assert frame.time_to_live == 456
    assert frame.metadata == b'Metadata on lease frame'
    assert frame.serialize() == data
