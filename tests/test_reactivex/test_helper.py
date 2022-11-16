import asyncio

import pytest
from reactivex import operators, Subject

from rsocket.reactivex.back_pressure_publisher import observable_from_async_generator, task_from_awaitable


@pytest.mark.parametrize('request_n, generate_n, expected_n',(
        (10, 5, 5),
        (5, 10, 5),
        (10, 10, 10),
        # (0, 10, 0), # operators.take(0) is problematic
))
async def test_helper(request_n, generate_n, expected_n):
    async def generator():
        for i in range(generate_n):
            yield i

    feedback = Subject()

    task = await task_from_awaitable(
        observable_from_async_generator(generator().__aiter__(), feedback).pipe(operators.take(request_n), operators.to_list())
    )

    feedback.on_next(request_n)

    result = await task

    assert len(result) == expected_n

    await asyncio.sleep(1) # wait for task to finish
