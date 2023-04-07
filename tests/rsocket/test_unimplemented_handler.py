from typing import Tuple

import pytest

from rsocket.awaitable.awaitable_rsocket import AwaitableRSocket
from rsocket.payload import Payload
from rsocket.rsocket_client import RSocketClient
from rsocket.rsocket_server import RSocketServer
from tests.rsocket.helpers import get_components


@pytest.mark.allow_error_log(regex_filter='(Protocol|Setup|Unknown) error')
async def test_request_response_not_implemented_by_server_by_default(pipe: Tuple[RSocketServer, RSocketClient]):
    payload = Payload(b'abc', b'def')
    server, client = get_components(pipe)

    with pytest.raises(RuntimeError) as exc_info:
        await client.request_response(payload)

    assert str(exc_info.value) == 'Not implemented'


@pytest.mark.allow_error_log(regex_filter='(Protocol|Setup|Unknown) error')
async def test_request_stream_not_implemented_by_server_by_default(pipe: Tuple[RSocketServer, RSocketClient]):
    payload = Payload(b'abc', b'def')
    server, client = get_components(pipe)

    with pytest.raises(RuntimeError) as exc_info:
        await AwaitableRSocket(client).request_stream(payload)

    assert str(exc_info.value) == 'Not implemented'


@pytest.mark.allow_error_log(regex_filter='(Protocol|Setup|Unknown) error')
async def test_request_channel_not_implemented_by_server_by_default(pipe: Tuple[RSocketServer, RSocketClient]):
    payload = Payload(b'abc', b'def')
    server, client = get_components(pipe)

    with pytest.raises(RuntimeError) as exc_info:
        await AwaitableRSocket(client).request_channel(payload)

    assert str(exc_info.value) == 'Not implemented'
