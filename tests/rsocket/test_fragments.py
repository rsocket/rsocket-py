from io import BytesIO

import pytest
from asyncstdlib import builtins

from rsocket.exceptions import RSocketFrameFragmentDifferentType
from rsocket.frame import PayloadFrame, RequestResponseFrame
from rsocket.frame_builders import to_payload_frame
from rsocket.frame_fragment_cache import FrameFragmentCache
from rsocket.frame_helpers import payload_to_n_size_fragments


@pytest.mark.parametrize('data, metadata, fragment_size, expected_frame_count', (
        (b'', b'123abc456def', 3, 5),
        (b'123abc456def', b'', 3, 5),
        (b'123abc', b'456def', 3, 5),
        (b'123abc89', b'456def', 3, 5),
        (b'123ab', b'456def', 3, 4),
        (b'123', b'456def', 3, 4),
        (b'123', b'45', 3, 2),
        (b'12', b'45', 3, 2),
        (b'12', b'456', 3, 2),
        (b'123', b'45', 3, 2),
        (b'123', b'456', 3, 3),
        (b'', b'', 3, 1),
))
async def test_fragment_only_metadata(data, metadata, fragment_size, expected_frame_count):
    fragments = await builtins.list(payload_to_n_size_fragments(BytesIO(data), BytesIO(metadata), fragment_size))

    assert len(fragments) == expected_frame_count

    cache = FrameFragmentCache()

    combined_payload = None
    for fragment in fragments:
        combined_payload = cache.append(to_payload_frame(1, fragment, complete=True))

    if metadata is None or len(metadata) == 0:
        assert combined_payload.metadata is None or combined_payload.metadata == b''
    else:
        assert combined_payload.metadata == metadata

    if data is None or len(data) == 0:
        assert combined_payload.data is None or combined_payload.data == b''
    else:
        assert combined_payload.data == data


async def test_frame_building_should_fail_if_inconsistent_frame_type():
    first_frame = PayloadFrame()
    first_frame.data = b'123'
    first_frame.flags_follows = True
    first_frame.flags_complete = False

    second_frame = RequestResponseFrame()
    second_frame.data = b'123'
    second_frame.flags_follows = False
    second_frame.flags_complete = True

    cache = FrameFragmentCache()

    cache.append(first_frame)

    with pytest.raises(RSocketFrameFragmentDifferentType):
        cache.append(second_frame)
