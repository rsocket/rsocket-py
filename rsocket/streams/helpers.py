from asyncio import Queue


async def async_generator_from_queue(queue: Queue, stop_value=None):
    while True:
        value = await queue.get()

        if value is stop_value:
            return
        else:
            yield value
            queue.task_done()
