from io import BytesIO

import pytest
from asyncstdlib import builtins

from rsocket.frame_builders import to_payload_frame
from rsocket.frame_fragment_cache import FrameFragmentCache
from rsocket.helpers import payload_to_n_size_fragments


@pytest.mark.parametrize('data, metadata, fragment_size, expected_frame_count', (
        (b'', b'123abc456def', 3, 5),
        (b'123abc456def', b'', 3, 5),
        (b'123abc', b'456def', 3, 5),
        (b'123abc89', b'456def', 3, 5),
        (b'123ab', b'456def', 3, 4),
        (b'', b'', 3, 1),
))
async def test_fragment_only_metadata(data, metadata, fragment_size, expected_frame_count):
    fragments = await builtins.list(payload_to_n_size_fragments(BytesIO(data), BytesIO(metadata), fragment_size))

    assert len(fragments) == expected_frame_count

    cache = FrameFragmentCache()

    combined_payload = None
    for fragment in fragments:
        combined_payload = cache.append(to_payload_frame(fragment, True, 1))

    if metadata is None or len(metadata) == 0:
        assert combined_payload.metadata is None or combined_payload.metadata == b''
    else:
        assert combined_payload.metadata == metadata

    if data is None or len(data) == 0:
        assert combined_payload.data is None or combined_payload.data == b''
    else:
        assert combined_payload.data == data
