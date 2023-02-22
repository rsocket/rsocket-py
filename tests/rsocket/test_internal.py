import asyncio
import logging
from collections import namedtuple
from weakref import WeakKeyDictionary

import pytest

from rsocket.logger import measure_runtime


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


def test_weak_ref():
    class S(str):
        pass

    d = WeakKeyDictionary()
    a = S('abc')
    d[a] = 1
    assert len(d) == 1

    del a

    assert len(d) == 0


async def test_range():
    async def loop(ii):
        for i in range(100):
            await asyncio.sleep(0)
            print(ii + str(i))

    await asyncio.gather(loop('a'), loop('b'))


def test_instantiate_named_tuple():
    routing = namedtuple('routing', ['tags', 'content'])
    count = 100000
    with measure_runtime() as result:
        for i in range(count):
            r = routing(b'abc', b'abcdefg')

    print(result.time.total_seconds() / count * 1000)


def test_instantiate_slotted_class():
    class routing:
        __slots__ = ['tags', 'content']

        def __init__(self, tags, content):
            self.tags = tags
            self.content = content

    count = 100000
    with measure_runtime() as result:
        for i in range(count):
            r = routing(b'abc', b'abcdefg')

    print(result.time.total_seconds() / count * 1000)

def test_instantiate_class():
    class routing:

        def __init__(self, tags, content):
            self.tags = tags
            self.content = content

    count = 100000
    with measure_runtime() as result:
        for i in range(count):
            r = routing(b'abc', b'abcdefg')

    print(result.time.total_seconds() / count * 1000)
