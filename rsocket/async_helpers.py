import asyncio


async def async_range(count: int):
    for i in range(count):
        yield i
        await asyncio.sleep(0)
