from typing import cast

import pytest

from rsocket.exceptions import RSocketFrameFragmentDifferentType
from rsocket.frame import PayloadFrame, RequestResponseFrame, FragmentableFrame, RequestStreamFrame, RequestChannelFrame
from rsocket.frame_builders import to_request_response_frame, to_request_stream_frame, to_request_channel_frame
from rsocket.frame_fragment_cache import FrameFragmentCache
from rsocket.payload import Payload
from tests.rsocket.helpers import create_data


def test_create_data():
    assert create_data(b'aaa', 3) == b'0aaa1aaa2aaa'


@pytest.mark.parametrize('data, metadata, fragment_size_bytes, expected_frame_count', (
        (b'', b'', 64, 1),  # empty payload
        (b'', create_data(b'123abc456def', 20), 64, 5),  # only data
        (create_data(b'123abc456def', 20), b'', 64, 5),  # only metadata
        (create_data(b'123abc456def', 20), create_data(b'123abc456def', 20, 55), 64, 6),  # metadata fits in first frame
        (create_data(b'123abc456def', 20), create_data(b'123abc456def', 20), 64, 10),  # mixed metadata/data frame
        (create_data(b'123abc456def', 4, 20), create_data(b'123abc456def', 4, 20), 64, 1),  # fit in one frame
        (create_data(b'123abc456def', 4, 20), create_data(b'123abc456def', 4, 25), 64, 1),  # fit in one frame exactly
))
async def test_fragmentation_payload(data, metadata, fragment_size_bytes, expected_frame_count):
    frame = PayloadFrame()
    frame.data = data
    frame.metadata = metadata
    frame.fragment_size_bytes = fragment_size_bytes

    def fragment_generator():
        while True:
            next_fragment = frame.get_next_fragment()

            if next_fragment is not None:
                yield next_fragment
            else:
                break

    fragments = list(fragment_generator())

    assert all(isinstance(fragment, PayloadFrame) for fragment in fragments)
    assert all(fragment.flags_follows for fragment in fragments[0:-1])
    assert fragments[-1].flags_follows is False

    assert len(fragments) == expected_frame_count

    cache = FrameFragmentCache()

    combined_payload = None
    for fragment in fragments:
        combined_payload = cache.append(cast(FragmentableFrame, fragment))

    if metadata is None or len(metadata) == 0:
        assert combined_payload.metadata is None or combined_payload.metadata == b''
    else:
        assert combined_payload.metadata == metadata

    if data is None or len(data) == 0:
        assert combined_payload.data is None or combined_payload.data == b''
    else:
        assert combined_payload.data == data


@pytest.mark.parametrize('request_builder, data, metadata, fragment_size_bytes, expected_frame_count, request_class', (
        (to_request_response_frame, b'', create_data(b'123abc456def', 20), 64, 5, RequestResponseFrame),
        (to_request_stream_frame, create_data(b'123abc456def', 20), b'', 64, 5, RequestStreamFrame),
        (to_request_channel_frame, create_data(b'123abc456def', 20), b'', 64, 5, RequestChannelFrame),
))
async def test_fragmentation_request(request_builder, data, metadata, fragment_size_bytes, expected_frame_count,
                                     request_class):
    frame = request_builder(1, Payload(data, metadata), fragment_size_bytes)

    def fragment_generator():
        while True:
            next_fragment = frame.get_next_fragment()

            if next_fragment is not None:
                yield next_fragment
            else:
                break

    fragments = list(fragment_generator())

    assert all(isinstance(fragment, PayloadFrame) for fragment in fragments[1:-1])
    assert all(fragment.flags_follows for fragment in fragments[0:-1])
    assert fragments[-1].flags_follows is False
    assert isinstance(fragments[0], request_class)

    assert len(fragments) == expected_frame_count

    cache = FrameFragmentCache()

    combined_frame = None
    for fragment in fragments:
        combined_frame = cache.append(cast(FragmentableFrame, fragment))

    assert isinstance(combined_frame, request_class)

    if metadata is None or len(metadata) == 0:
        assert combined_frame.metadata is None or combined_frame.metadata == b''
    else:
        assert combined_frame.metadata == metadata

    if data is None or len(data) == 0:
        assert combined_frame.data is None or combined_frame.data == b''
    else:
        assert combined_frame.data == data


async def test_frame_building_should_fail_if_inconsistent_frame_type():
    first_frame = RequestResponseFrame()
    first_frame.data = b'123'
    first_frame.flags_follows = True
    first_frame.flags_complete = False

    second_frame = RequestChannelFrame()
    second_frame.data = b'123'
    second_frame.flags_follows = False
    second_frame.flags_complete = True

    cache = FrameFragmentCache()

    cache.append(first_frame)

    with pytest.raises(RSocketFrameFragmentDifferentType):
        cache.append(second_frame)
