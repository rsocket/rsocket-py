import pytest

from rsocket.exceptions import RSocketUnknownMimetype, RSocketMimetypeTooLong
from rsocket.extensions.mimetypes import WellKnownMimeTypes
from rsocket.helpers import serialize_well_known_encoding


def test_mimetype_raise_exception_on_unknown_type():
    with pytest.raises(RSocketUnknownMimetype) as exc_info:
        WellKnownMimeTypes.require_by_id(99999)

    assert exc_info.value.mimetype_id == 99999


def test_serialize_well_known_encoding_too_long():
    with pytest.raises(RSocketMimetypeTooLong):
        serialize_well_known_encoding(b'1' * 1000, WellKnownMimeTypes.get_by_name)
