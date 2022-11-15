from asyncio import Queue


async def queue_to_async_generator(queue: Queue, stop_value=None):
    while True:
        value = await queue.get()
        if value is stop_value:
            return
        else:
            yield value
