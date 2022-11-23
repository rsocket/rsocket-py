import pytest

from rsocket.payload import Payload
from rsocket.reactivex.reactivex_handler import BaseReactivexHandler


async def test_reactivex_handler():
    handler = BaseReactivexHandler()

    with pytest.raises(Exception):
        await handler.request_channel(Payload())

    with pytest.raises(Exception):
        await handler.request_response(Payload())

    with pytest.raises(Exception):
        await handler.request_channel(Payload())
