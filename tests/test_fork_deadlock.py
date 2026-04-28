# Copyright (c) Microsoft Corporation.
# Licensed under the MIT License.
"""Reproduce: ``ProcessPoolExecutor`` with ``fork`` deadlocks when parent holds locks.

Bug
---
``ProcessPool._create_pool()`` creates a ``ProcessPoolExecutor`` without
specifying ``mp_context``, so on Linux it defaults to ``fork``.  When the
parent process has background threads that hold ``threading.Lock`` instances
(as the Azure SDK's ``ManagedIdentityCredential`` does internally for token
caching, or ``aiohttp`` does for connection pooling), the forked child
inherits copies of those locks **in the locked state**.  Any child code that
tries to acquire the same lock deadlocks.

In production this manifests as the MRCC task hanging indefinitely when it
tries to download a checkpoint via ``BlobServiceClient`` with
``ManagedIdentityCredential`` — the IMDS token acquisition never completes.

Reproduction
------------
We simulate the Azure SDK's internal state by holding a *module-level*
``threading.Lock`` in a background thread in the parent.  A function
submitted to the ``ProcessPoolExecutor`` tries to acquire the same lock.
With ``fork``, the child's copy of the lock is already held → deadlock.
With ``forkserver`` or ``spawn``, the child starts clean → no deadlock.
"""

import threading
import time

from ai4s.jobq.work import ProcessPool

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


async def test_fork_deadlock_with_held_lock():
    """ProcessPool using ``fork`` deadlocks when a module-level lock is held
    in the parent.

    This test should FAIL (timeout / hang) with the current ``fork``-based
    pool and PASS after switching to ``forkserver`` or ``spawn``.
    """
    # Hold the module-level lock in a background thread for the duration
    # of the test, simulating Azure SDK internal state.
    stop = threading.Event()

    def hold_lock():
        _MODULE_LOCK.acquire()
        try:
            stop.wait()
        finally:
            _MODULE_LOCK.release()

    bg = threading.Thread(target=hold_lock, daemon=True)
    bg.start()

    # Give the thread time to acquire the lock.
    time.sleep(0.2)
    assert _MODULE_LOCK.locked(), "Background thread should be holding the lock"

    deadlocked = False
    try:
        async with ProcessPool(pool_size=1) as pool:
            # Submit a task that needs the lock.  With fork, this deadlocks
            # because the child inherits the lock in a locked state.
            # The child uses Lock.acquire(timeout=5), so it will raise
            # TimeoutError after 5s rather than blocking forever.
            result = await pool.submit(_task_that_needs_lock)
            assert result == "ok"
    except RuntimeError as exc:
        # The child raised TimeoutError which gets wrapped by the executor.
        if "Could not acquire" in str(exc) or "deadlock" in str(exc.__cause__ or ""):
            deadlocked = True
        else:
            raise
    except TimeoutError:
        deadlocked = True
    finally:
        stop.set()
        bg.join(timeout=2)

    assert not deadlocked, (
        "Pool task deadlocked: child process could not acquire _MODULE_LOCK "
        "because fork inherited it in a locked state.\n"
        "This confirms the fork-deadlock bug: ProcessPoolExecutor uses "
        "'fork' by default, inheriting the parent's locked "
        "threading.Lock into the child process."
    )
