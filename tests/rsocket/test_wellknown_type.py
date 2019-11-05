import pytest

from rsocket.wellknown_mimetype import WellKnowMimeTypes


def test_enum():
    for well_known_type in WellKnowMimeTypes:
        print(well_known_type.name)


def test_from_identifier():
    well_know_type = WellKnowMimeTypes.from_identifier(0x01)
    assert well_know_type.name == "application/cbor"


def test_from_name():
    well_know_type = WellKnowMimeTypes.from_mime_type("application/cbor")
    assert well_know_type.identifier == 1


def test_add_wellknown():
    mime_type = "application/x.rsocket.data-encoding.v0"
    WellKnowMimeTypes.add_wellknown(mime_type, 0x40)
    assert WellKnowMimeTypes.from_mime_type(mime_type).identifier == 0x40
