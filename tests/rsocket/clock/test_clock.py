import asyncio

import pytest
from asyncio.test_utils import TestLoop as EventLoop


@pytest.mark.asyncio
@asyncio.coroutine
def test_sleep(event_loop: EventLoop):
    def near(x, y, relative=5):
        return y - relative < x < y + relative

    start = event_loop.time()
    yield from asyncio.sleep(1000000)
    assert near(event_loop.time(), 1000000)
    yield from asyncio.sleep(100)
    assert near(event_loop.time() - start, 1000000 + 100)

