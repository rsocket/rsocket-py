from typing import cast

import asyncstdlib

from rsocket.extensions.authentication import AuthenticationSimple
from rsocket.extensions.authentication_content import AuthenticationContent
from rsocket.extensions.composite_metadata import CompositeMetadata


async def test_decode_spring_demo_auth():
    metadata = bytearray(
        b'\xfe\x00\x00\r\x0cshell-client"message/x.rsocket.authentication.v0\x00\x00\x0b\x80\x00\x04userpass')

    composite_metadata = CompositeMetadata()
    composite_metadata.parse(metadata)

    assert len(composite_metadata.items) == 2

    composite_item = cast(AuthenticationContent, composite_metadata.items[1])
    authentication = cast(AuthenticationSimple, composite_item.authentication)
    assert authentication.username == b'user'
    assert authentication.password == b'pass'


async def test_multiple_frames(frame_parser):
    data = b'\x00\x00\x06\x00\x00\x00\x7b\x24\x00'
    data += b'\x00\x00\x13\x00\x00\x26\x6a\x2c\x00\x00\x00\x02\x04\x77\x65\x69'
    data += b'\x72\x64\x6e\x65\x73\x73'
    data += b'\x00\x00\x06\x00\x00\x00\x7b\x24\x00'
    data += b'\x00\x00\x06\x00\x00\x00\x7b\x24\x00'
    data += b'\x00\x00\x06\x00\x00\x00\x7b\x24\x00'
    frames = await asyncstdlib.builtins.list(frame_parser.receive_data(data))
    assert len(frames) == 5
