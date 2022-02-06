import asyncio
import logging

import pytest


async def test_reader(event_loop: asyncio.AbstractEventLoop):
    stream = asyncio.StreamReader(loop=event_loop)
    stream.feed_data(b'data')
    stream.feed_eof()
    data = await stream.read()
    assert data == b'data'


@pytest.mark.xfail(
    reason='This is testing the fixture which should cause the test to fail if there is an error log')
async def test_fail_on_error_log(fail_on_error_log):
    logging.error("this should not happen")
