from typing import Tuple

import pytest

from rsocket.awaitable.awaitable_rsocket import AwaitableRSocket
from rsocket.payload import Payload
from rsocket.rsocket_client import RSocketClient
from rsocket.rsocket_server import RSocketServer


async def test_request_response_not_implemented_by_server_by_default(pipe: Tuple[RSocketServer, RSocketClient]):
    payload = Payload(b'abc', b'def')
    server, client = pipe

    with pytest.raises(RuntimeError) as exc_info:
        await client.request_response(payload)

    assert str(exc_info.value) == 'Not implemented'


async def test_request_stream_not_implemented_by_server_by_default(pipe: Tuple[RSocketServer, RSocketClient]):
    payload = Payload(b'abc', b'def')
    server, client = pipe

    with pytest.raises(RuntimeError) as exc_info:
        await AwaitableRSocket(client).request_stream(payload)

    assert str(exc_info.value) == 'Not implemented'


async def test_request_channel_not_implemented_by_server_by_default(pipe: Tuple[RSocketServer, RSocketClient]):
    payload = Payload(b'abc', b'def')
    server, client = pipe

    with pytest.raises(RuntimeError) as exc_info:
        await AwaitableRSocket(client).request_channel(payload)

    assert str(exc_info.value) == 'Not implemented'
