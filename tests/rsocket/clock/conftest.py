from asyncio.test_utils import TestLoop

import pytest


@pytest.yield_fixture()
def event_loop():
    def gen():
        """The ``gen`` argument to :class: TestLoop's constructor.

        Note: the class isn't documented.  From code reading, it appears that
        the ``when`` values are sent to the generator, which yields advance
        increments, stopping the generation when ``when == 0``.
        """
        advance = 0
        while True:
            when = yield advance
            if not when: break
            advance = when - loop.time()

    loop = TestLoop(gen)
    yield loop
    loop.close()
