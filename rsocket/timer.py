import asyncio


class Timer:
    def __init__(self, timeout, callback, callback_args=()):
        self._timeout = timeout
        self._callback = callback
        self._task = asyncio.ensure_future(self._job(callback_args))

    async def _job(self, callback_args):
        await asyncio.sleep(self._timeout)
        await self._callback(*callback_args)

    def cancel(self):
        self._task.cancel()
