import pytest

from rsocket.payload import Payload
from rsocket.rx_support.rx_handler import BaseRxHandler


async def test_rx_handler():
    handler = BaseRxHandler()

    with pytest.raises(Exception):
        await handler.request_channel(Payload())

    with pytest.raises(Exception):
        await handler.request_response(Payload())

    with pytest.raises(Exception):
        await handler.request_stream(Payload())
