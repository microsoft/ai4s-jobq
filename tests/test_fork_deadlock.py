# Copyright (c) Microsoft Corporation.
# Licensed under the MIT License.
"""Reproduce fork-related deadlocks in ``ProcessPool``.

Bug 1 — ProcessPoolExecutor with ``fork``
------------------------------------------
``ProcessPool._create_pool()`` creates a ``ProcessPoolExecutor`` without
specifying ``mp_context``, so on Linux it defaults to ``fork``.  When the
parent process has background threads that hold ``threading.Lock`` instances
(as the Azure SDK's ``ManagedIdentityCredential`` does internally for token
caching, or ``aiohttp`` does for connection pooling), the forked child
inherits copies of those locks **in the locked state**.  Any child code that
tries to acquire the same lock deadlocks.

Bug 2 — ``multiprocessing.Manager()`` with ``fork``
----------------------------------------------------
Even after fixing the pool executor, ``ProcessPool.__init__`` creates a
``multiprocessing.Manager()`` using the *default* start method (``fork`` on
Linux).  The Manager spawns a server process that inherits the parent's
locked state.  ``ShellCommandProcessor`` children forward logs via a
``Manager().Queue()`` proxy — when the Manager server is deadlocked, every
``queue.put()`` in the child hangs indefinitely.

Reproduction
------------
We simulate the Azure SDK's internal state by holding a *module-level*
``threading.Lock`` in a background thread in the parent, then verify that
both pool task execution and Manager-based IPC work without deadlocking.
"""

import asyncio
import threading
from functools import partial
from typing import TYPE_CHECKING

from ai4s.jobq.work import ProcessPool

if TYPE_CHECKING:
    import multiprocessing

# ── Module-level lock simulating Azure SDK internal state ───────────────
# In the real scenario this is e.g.
#   azure.identity._credentials.managed_identity.ManagedIdentityCredential._lock
# or an internal aiohttp connector lock.
_MODULE_LOCK = threading.Lock()


def _task_that_needs_lock() -> str:
    """Function executed in a pool child process.

    Tries to acquire the module-level lock.  With ``fork``, the child
    inherits a copy of the lock that is already held by the (now-gone)
    background thread → deadlock.
    """
    acquired = _MODULE_LOCK.acquire(timeout=5)
    if not acquired:
        raise TimeoutError(
            "Could not acquire _MODULE_LOCK in child process within 5s — "
            "deadlock caused by fork inheriting a locked threading.Lock"
        )
    _MODULE_LOCK.release()
    return "ok"


def _task_that_writes_to_queue(q: "multiprocessing.Queue[str]") -> str:
    """Function executed in a pool child that writes to a Manager queue.

    This mimics ``ShellCommandProcessor._subprocess_call`` which forwards
    log lines via ``log_msg_queue.put()``.  If the Manager server process
    was forked from a parent with held locks, the server may be deadlocked
    and the ``put()`` call will hang.

    Uses the same tuple format as the real log forwarding:
    ``(level: int, job_id: str, message: str)``.
    """
    import logging

    try:
        q.put((logging.INFO, "test-job", "hello from child"), timeout=5)
    except Exception as e:
        raise TimeoutError(
            f"Could not put to Manager queue within 5s — "
            f"Manager server likely deadlocked due to fork: {e}"
        ) from e
    return "ok"


# ── Helpers ─────────────────────────────────────────────────────────────


class _LockHolder:
    """Context manager that holds ``_MODULE_LOCK`` in a background thread."""

    def __init__(self) -> None:
        self._stop = threading.Event()
        self._thread: threading.Thread | None = None

    def __enter__(self) -> "_LockHolder":
        def hold():
            _MODULE_LOCK.acquire()
            try:
                self._stop.wait()
            finally:
                _MODULE_LOCK.release()

        self._thread = threading.Thread(target=hold, daemon=True)
        self._thread.start()
        return self

    def __exit__(self, *args) -> None:
        self._stop.set()
        if self._thread:
            self._thread.join(timeout=2)


async def _wait_for_lock() -> None:
    """Async-friendly wait until ``_MODULE_LOCK`` is held."""
    for _ in range(50):
        if _MODULE_LOCK.locked():
            return
        await asyncio.sleep(0.05)
    raise AssertionError("Background thread did not acquire _MODULE_LOCK in time")


# ── Tests ───────────────────────────────────────────────────────────────


async def test_fork_deadlock_with_held_lock():
    """Pool child must not deadlock on a module-level lock held in the parent.

    Validates that the pool executor uses a safe start method (not ``fork``).
    """
    with _LockHolder():
        await _wait_for_lock()

        deadlocked = False
        try:
            async with ProcessPool(pool_size=1) as pool:
                result = await pool.submit(_task_that_needs_lock)
                assert result == "ok"
        except (RuntimeError, TimeoutError) as exc:
            msg = str(exc) + str(getattr(exc, "__cause__", "") or "")
            if "Could not acquire" in msg or "deadlock" in msg:
                deadlocked = True
            else:
                raise

        assert not deadlocked, (
            "Pool task deadlocked: child process could not acquire _MODULE_LOCK "
            "because fork inherited it in a locked state. "
            "ProcessPoolExecutor must use a non-fork start method."
        )


async def test_manager_queue_not_deadlocked():
    """Manager-based IPC must work when parent threads hold locks.

    This is the production failure path: ``ShellCommandProcessor`` children
    forward logs via ``ProcessPool.log_msg_queue`` (a ``Manager().Queue()``
    proxy).  If the Manager server was forked from a parent with held locks,
    the server deadlocks and every ``queue.put()`` hangs.
    """
    with _LockHolder():
        await _wait_for_lock()

        deadlocked = False
        try:
            async with ProcessPool(pool_size=1) as pool:
                assert pool.log_msg_queue is not None
                result = await pool.submit(partial(_task_that_writes_to_queue, pool.log_msg_queue))
                assert result == "ok"
        except (RuntimeError, TimeoutError) as exc:
            msg_str = str(exc) + str(getattr(exc, "__cause__", "") or "")
            if "Manager" in msg_str or "deadlock" in msg_str or "Could not put" in msg_str:
                deadlocked = True
            else:
                raise

        assert not deadlocked, (
            "Manager queue deadlocked: child could not put() to the Manager "
            "queue because the Manager server process inherited locked state "
            "from the parent via fork. Manager must use a non-fork start method."
        )
