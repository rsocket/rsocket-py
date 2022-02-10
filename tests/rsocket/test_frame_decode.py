from rsocket.extensions.composite_metadata import CompositeMetadata


async def test_decode_spring_demo_auth():
    metadata = bytearray(
        b'\xfe\x00\x00\r\x0cshell-client"message/x.rsocket.authentication.v0\x00\x00\x0b\x80\x00\x04userpass')

    composite_metadata = CompositeMetadata()
    composite_metadata.parse(metadata)

    assert len(composite_metadata.items) == 2

    authentication = composite_metadata.items[1].authentication
    assert authentication.username == b'user'
    assert authentication.password == b'pass'
