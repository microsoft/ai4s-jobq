# Copyright (c) Microsoft Corporation.
# Licensed under the MIT License.
import asyncio
import queue
import typing as ty
from concurrent.futures import ThreadPoolExecutor
from functools import partial
from multiprocessing import Manager, cpu_count


def AsyncProcessQueue(maxsize: int = 0) -> "_ProcQueue":
    m = Manager()
    q = m.Queue(maxsize=maxsize)
    return _ProcQueue(q)


class _ProcQueue:
    def __init__(self, q: "queue.Queue[ty.Any]"):
        self._queue = q
        self._real_executor: ty.Optional[ThreadPoolExecutor] = None
        self._cancelled_join = False

    @property
    def _executor(self) -> ThreadPoolExecutor:
        if not self._real_executor:
            self._real_executor = ThreadPoolExecutor(max_workers=cpu_count())
        return self._real_executor

    def __getstate__(self) -> ty.Dict[str, ty.Any]:
        self_dict = self.__dict__
        self_dict["_real_executor"] = None
        return self_dict

    def __getattr__(self, name: str) -> ty.Any:
        if name in [
            "qsize",
            "empty",
            "full",
            "put",
            "put_nowait",
            "get",
            "get_nowait",
            "close",
        ]:
            return getattr(self._queue, name)
        else:
            raise AttributeError(
                "'%s' object has no attribute '%s'" % (self.__class__.__name__, name)
            )

    async def coro_put(self, item: ty.Any) -> ty.Any:
        loop = asyncio.get_event_loop()
        return await loop.run_in_executor(self._executor, self.put, item)

    async def coro_get(self, timeout: ty.Optional[int]) -> ty.Any:
        loop = asyncio.get_event_loop()
        return await loop.run_in_executor(self._executor, partial(self.get, timeout=timeout))

    def cancel_join_thread(self) -> None:
        self._cancelled_join = True
        self._queue.cancel_join_thread()  # type: ignore

    def join_thread(self) -> None:
        self._queue.join_thread()  # type: ignore
        if self._real_executor and not self._cancelled_join:
            self._real_executor.shutdown()
