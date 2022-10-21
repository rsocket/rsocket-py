import sys
from asyncio import Queue, QueueEmpty


class QueuePeekable(Queue):

    async def peek(self):
        """Peek the next item in the queue.

        If queue is empty, wait until an item is available.
        """
        while self.empty():
            getter = self._get_loop().create_future()
            self._getters.append(getter)
            try:
                await getter
            except Exception:
                getter.cancel()  # Just in case getter is not done yet.
                try:
                    # Clean self._getters from canceled getters.
                    self._getters.remove(getter)
                except ValueError:
                    # The getter could be removed from self._getters by a
                    # previous put_nowait call.
                    pass
                if not self.empty() and not getter.cancelled():
                    # We were woken up by put_nowait(), but can't take
                    # the call.  Wake up the next in line.
                    self._wakeup_next(self._getters)
                raise
        return self.peek_nowait()

    def peek_nowait(self):
        """Peek the next item in the queue.

        Return an item if one is immediately available, else raise QueueEmpty.
        """
        if self.empty():
            raise QueueEmpty

        item = self._queue[0]
        self._wakeup_next(self._putters)
        return item


if sys.version_info < (3, 10):
    class QueuePeekableBackwardCompatible(QueuePeekable):
        def _get_loop(self):
            return self._loop


    QueuePeekable = QueuePeekableBackwardCompatible
