from asyncio import Queue

from rsocket.streams.helpers import async_generator_from_queue


async def test_async_generator_from_queue():
    queue = Queue()

    for i in range(10):
        queue.put_nowait(i)

    queue.put_nowait(None)

    async def collect():
        results = []
        async for i in async_generator_from_queue(queue):
            results.append(i)

        return results

    r = await collect()

    assert r == list(range(10))
