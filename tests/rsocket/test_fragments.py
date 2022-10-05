from typing import cast

import pytest

from rsocket.exceptions import RSocketFrameFragmentDifferentType
from rsocket.frame import PayloadFrame, RequestResponseFrame, FragmentableFrame
from rsocket.frame_fragment_cache import FrameFragmentCache


@pytest.mark.parametrize('data, metadata, fragment_size, expected_frame_count', (
        (b'', b'123abc456def', 3, 4),
        (b'123abc456def', b'', 3, 4),
        (b'123abc', b'456def', 3, 4),
        (b'123abc89', b'456def', 3, 5),
        (b'123ab', b'456def', 3, 4),
        (b'123', b'456def', 3, 3),
        (b'123', b'45', 3, 2),
        (b'12', b'45', 3, 2),
        (b'12', b'456', 3, 2),
        (b'123', b'45', 3, 2),
        (b'123', b'456', 3, 2),
))
async def test_fragment_only_metadata(data, metadata, fragment_size, expected_frame_count):
    frame = PayloadFrame()
    frame.data = data
    frame.metadata = metadata
    frame.fragment_size = fragment_size

    def fragment_generator():
        while True:
            next_fragment = frame.get_next_fragment()

            if next_fragment is not None:
                yield next_fragment
            else:
                break

    fragments = list(fragment_generator())

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
