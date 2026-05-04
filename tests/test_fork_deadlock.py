# Copyright (c) Microsoft Corporation.
# Licensed under the MIT License.
"""Regression tests for fork-related deadlocks in ``ProcessPool``.

Background
----------
On Linux the default multiprocessing start method is ``fork``, which
clones the parent's memory into the child — including every lock in its
current state.  C-level locks held by parent threads at the moment of
``fork`` (for example inside Azure SDK credential caches, MSAL token
refresh, or OpenSSL) are stuck locked in the child because the owning
thread does not exist there.  Any child code that acquires such a lock
deadlocks.

Python 3.12 made stdlib locks (``threading.Lock``, ``logging._lock``)
fork-safe via ``os.register_at_fork``, but C extension locks remain
vulnerable.  We therefore cannot reproduce the deadlock with pure Python
in 3.12+ — instead we verify the fix (use ``forkserver`` or ``spawn``
instead of ``fork``) by checking the start-method invariant and running
a positive integration test.

The fix in ``ProcessPool`` uses ``_safe_mp_context()`` which returns
``forkserver`` on Linux/macOS and ``spawn`` on Windows — neither
inherits the parent's memory, so C-level locks are never poisoned.
"""

import asyncio
import logging
import sys
import threading

import pytest

from ai4s.jobq.work import ProcessPool, _safe_mp_context

_log = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

_CHILD_TIMEOUT = 30  # seconds — generous; forkserver child cold-start can
# be slow on busy CI runners, especially when the parent has heavy imports
# (azure-ai-ml etc.) that the forkserver has to re-execute.


def _child_that_logs() -> str:
    """Function executed in a pool child.  Calls ``logging.info()``.

    With ``fork`` + C-level lock contention in the parent this can
    deadlock.  With ``forkserver`` or ``spawn`` it always succeeds.
    """
    _log.info("hello from child process")
    return "ok"


class _LogSpammer:
    """Context manager that floods ``logging`` from background threads.

    Keeps ``logging._lock`` contended so that a ``fork``-based start
    method would be at risk of inheriting it in a locked state (though
    Python 3.12+ re-initialises it via ``register_at_fork``).
    """

    def __init__(self, n_threads: int = 10) -> None:
        self._stop = threading.Event()
        self._threads: list[threading.Thread] = []
        self._n_threads = n_threads

    def __enter__(self) -> "_LogSpammer":
        for _ in range(self._n_threads):
            t = threading.Thread(target=self._spam, daemon=True)
            t.start()
            self._threads.append(t)
        return self

    def _spam(self) -> None:
        i = 0
        while not self._stop.is_set():
            _log.debug("spam %d", i)
            i += 1

    def __exit__(self, *args: object) -> None:
        self._stop.set()
        for t in self._threads:
            t.join(timeout=2)


# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------


def test_safe_mp_context_is_not_fork():
    """``_safe_mp_context()`` must never return the ``fork`` method.

    This is the core invariant that prevents the production deadlock.
    C-level locks in Azure SDK / MSAL / OpenSSL are not registered with
    ``os.register_at_fork`` and will deadlock children started via
    ``fork`` when background threads hold them.
    """
    ctx = _safe_mp_context()
    method = ctx.get_start_method()
    assert method != "fork", (
        f"_safe_mp_context() returned '{method}'; "
        "expected 'forkserver' or 'spawn' to avoid inheriting C-level "
        "locks from parent threads"
    )
    if sys.platform == "win32":
        assert method == "spawn"
    else:
        assert method == "forkserver"


async def test_processpool_children_can_log_under_contention():
    """``ProcessPool`` children must be able to log even when the parent
    has heavy logging contention from background threads.

    The pool is created **before** the log-spamming threads start, so
    the ``forkserver`` process is spawned from a clean, single-threaded
    state.  All subsequent children are forked from the clean forkserver,
    not the multi-threaded parent.
    """
    async with ProcessPool(pool_size=1) as pool:
        # Start logging contention AFTER the pool (and forkserver) is up.
        with _LogSpammer(n_threads=10):
            await asyncio.sleep(0.05)

            # A handful of successful child invocations is enough regression
            # coverage: a fork-deadlock would hang on the very first call.
            # Twenty attempts at a 5s wallclock timeout each was found to
            # be flaky on Python 3.11 under heavy parent imports.
            for attempt in range(5):
                try:
                    result = await asyncio.wait_for(
                        pool.submit(_child_that_logs),
                        timeout=_CHILD_TIMEOUT,
                    )
                    assert result == "ok"
                except (asyncio.TimeoutError, TimeoutError) as exc:
                    pytest.fail(
                        f"Pool child hung on attempt {attempt + 1}/5: {exc}\n"
                        "ProcessPool must use a non-fork start method so "
                        "children are not affected by parent thread state."
                    )
