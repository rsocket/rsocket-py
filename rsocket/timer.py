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


class IntervalTimer:
    def __init__(self, interval, callback, callback_args=()):
        self._interval = interval
        self._callback = callback
        self._ok = True
        self._task = asyncio.ensure_future(self._job(callback_args))

    async def _job(self, callback_args):
        try:
            while self._ok:
                await asyncio.sleep(self._interval)
                await self._callback(*callback_args)
        except Exception as ex:
            print(ex)

    def cancel(self):
        self._ok = False
        self._task.cancel()