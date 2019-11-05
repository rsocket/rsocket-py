import pytest

from rsocket.composite_metadata import CompositeMetadata, TaggingMetadata


def test_byte_array():
    array = bytearray()
    mime_id = 0x01
    # append one byte
    array.append(mime_id | 0x80)
    # append int
    length = 3
    array += length.to_bytes(3, byteorder='big')
    # append bytes
    array += b'hello'
    assert len(array) == 9
    assert array[0] == 129
    assert array[8] == 0x6F


def test_composite_metadata():
    composite_metadata = CompositeMetadata()
    composite_metadata.add_wellknown_metadata(0x01, b'123')
    composite_metadata.add_custom_metadata("a/b", b'123')
    composite_metadata.add_custom_metadata("a/c", b'456')
    composite_metadata.add_wellknown_metadata(0x02, b'123')
    for metadata in composite_metadata:
        print(metadata.get_mime_type(), ":", metadata.get_content().decode("ascii"))

    composite_metadata_2 = CompositeMetadata(composite_metadata.get_source())
    for metadata in composite_metadata_2:
        print(metadata.get_mime_type(), ":", metadata.get_content().decode("ascii"))

    composite_metadata.rewind()
    # convert to metadata dict
    all_metadata = {metadata.get_mime_type(): metadata.get_content() for metadata in composite_metadata}
    print(all_metadata)


def test_tagging_metadata():
    tagging_metadata = TaggingMetadata.from_tags("a/c", ["first", "second"])
    assert tagging_metadata.__next__() == "first"
    assert tagging_metadata.__next__() == "second"
