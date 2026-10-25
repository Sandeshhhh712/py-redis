"""Wake-up notification used by blocking list commands."""

import asyncio


class Notifier:
    """A tiny key-scoped async notification bus.

    Blocking commands register a future for the keys they care about and are
    woken whenever one of those keys changes.  Everything runs on the event
    loop, so there is no lock needed here.
    """

    def __init__(self):
        self._waiters = {}

    def notify(self, db_index, key):
        for future in list(self._waiters.get((db_index, key), ())):
            if not future.done():
                future.set_result(None)

    def notify_db(self, db_index):
        for (index, _key), futures in list(self._waiters.items()):
            if index != db_index:
                continue
            for future in list(futures):
                if not future.done():
                    future.set_result(None)

    async def wait(self, db_index, keys, timeout):
        """Wait until one of ``keys`` changes or ``timeout`` seconds pass.

        ``timeout=None`` waits forever.  A timeout of 0 is handled by callers
        (it means "do not block").
        """
        future = asyncio.get_running_loop().create_future()
        slots = [(db_index, key) for key in keys]
        for slot in slots:
            self._waiters.setdefault(slot, set()).add(future)
        try:
            await asyncio.wait_for(future, timeout)
        except asyncio.TimeoutError:
            pass
        finally:
            for slot in slots:
                futures = self._waiters.get(slot)
                if futures is not None:
                    futures.discard(future)
                    if not futures:
                        del self._waiters[slot]
