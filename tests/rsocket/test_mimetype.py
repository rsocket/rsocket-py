import pytest

from rsocket.exceptions import RSocketUnknownMimetype
from rsocket.extensions.mimetypes import WellKnownMimeTypes


def test_mimetype_raise_exception_on_unknown_type():
    with pytest.raises(RSocketUnknownMimetype) as exc_info:
        WellKnownMimeTypes.require_by_id(99999)

    assert exc_info.value.mimetype_id == 99999
