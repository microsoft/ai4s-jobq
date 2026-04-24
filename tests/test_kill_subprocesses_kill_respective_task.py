# Copyright (c) Microsoft Corporation.
# Licensed under the MIT License.
"""Demonstrate the ``_kill_subprocesses`` blast radius in ``ProcessPool``.

When does this happen in production?
-------------------------------------
1. Multiple workers (``ai4s-jobq worker -n N``) share a single
   ``ProcessPool`` (size = N).
2. A Service Bus message lock is lost (for example, the lock expires
   because renewal failed due to a transient network outage, or the
   message is dead-lettered after exceeding ``max_delivery_count``).
3. ``pull_and_execute`` detects the lock loss and cancels the
   ``callback_task`` for that ONE worker.
4. ``ProcessPool.submit()`` catches ``CancelledError`` and calls
   ``_kill_subprocesses()``.
5. ``_kill_subprocesses()`` iterates **all** ``ProcessPoolExecutor``
   children and sends SIGUSR1 to **every one of them** — not just the
   pool process running the cancelled task.  (The code itself notes:
   "ideally, we should only SIGTERM the process submitted above, but
   we don't know its PID".)
6. Every pool child's ``handle_shutdown_signal`` forwards SIGTERM to
   its shell subprocess → all running tasks die.

From the user's perspective one task had a transient issue and then
all other running tasks were killed as collateral damage.

The test below exercises the ProcessPool / ShellCommandProcessor
directly — no Service Bus needed.  It launches 3 concurrent tasks,
cancels just ONE of them, and verifies that the other two also
receive SIGTERM (the undesired blast-radius behavior).
"""

import asyncio
import os
import tempfile
import textwrap
import time

import pytest

from ai4s.jobq.entities import WorkerCanceled
from ai4s.jobq.work import ShellCommandProcessor


def _ts():
    return time.strftime("%H:%M:%S")


NUM_POOL_WORKERS = 3
TASK_DURATION_S = 30


async def test_kill_subprocesses_sends_sigterm_to_all_workers():
    """Cancelling ONE task in a ProcessPool sends SIGTERM to ALL running tasks.

    This is the blast-radius bug: ``_kill_subprocesses`` iterates all pool
    children instead of targeting only the one whose task was cancelled.
    After fixing, only the cancelled task should receive SIGTERM while the
    others continue running to completion.
    """
    marker_dir = tempfile.mkdtemp(prefix="jobq_blast_radius_")

    async with ShellCommandProcessor(num_workers=NUM_POOL_WORKERS) as proc:
        # Build shell commands that record their fate to marker files.
        # Each task: traps SIGTERM, writes RUNNING, sleeps, then writes COMPLETED.
        tasks: list[asyncio.Task] = []
        for i in range(NUM_POOL_WORKERS):
            marker = os.path.join(marker_dir, f"task_{i}.status")
            script = textwrap.dedent(f"""\
                trap 'echo SIGTERM > {marker}; exit 1' TERM
                echo RUNNING > {marker}
                sleep {TASK_DURATION_S}
                echo COMPLETED > {marker}
            """).strip()
            task = asyncio.create_task(
                proc(cmd=script, _job_id=f"task_{i}"),
                name=f"task-{i}",
            )
            tasks.append(task)

        # Wait for all tasks to actually start running in their pool children.
        for attempt in range(20):
            await asyncio.sleep(0.5)
            running = sum(
                1
                for i in range(NUM_POOL_WORKERS)
                if os.path.exists(os.path.join(marker_dir, f"task_{i}.status"))
                and open(os.path.join(marker_dir, f"task_{i}.status")).read().strip() == "RUNNING"
            )
            if running == NUM_POOL_WORKERS:
                break
        else:
            for t in tasks:
                t.cancel()
            pytest.fail(f"Only {running}/{NUM_POOL_WORKERS} tasks started within 10 s")

        print(f"[{_ts()}] All {NUM_POOL_WORKERS} tasks confirmed RUNNING", flush=True)

        # ── Cancel exactly ONE task (simulates lock-loss cancellation) ──
        tasks[0].cancel()
        try:
            await tasks[0]
        except (asyncio.CancelledError, WorkerCanceled, RuntimeError):
            pass
        print(f"[{_ts()}] task_0 cancelled", flush=True)

        # ── Wait for the other tasks to settle ──────────────────────────
        for t in tasks[1:]:
            try:
                await asyncio.wait_for(t, timeout=TASK_DURATION_S + 10)
            except (asyncio.CancelledError, WorkerCanceled, RuntimeError, TimeoutError):
                pass

    # ── Read marker files ───────────────────────────────────────────────
    statuses = {}
    for i in range(NUM_POOL_WORKERS):
        marker = os.path.join(marker_dir, f"task_{i}.status")
        if os.path.exists(marker):
            statuses[i] = open(marker).read().strip()
        else:
            statuses[i] = "MISSING"
    print(f"[{_ts()}] Task statuses: {statuses}", flush=True)

    # task_0 was explicitly cancelled — it should have received SIGTERM.
    assert statuses[0] == "SIGTERM", (
        f"task_0 was cancelled but status is {statuses[0]!r} (expected 'SIGTERM')"
    )

    # BUG: the other tasks ALSO received SIGTERM because _kill_subprocesses
    # sends SIGUSR1 to ALL pool children, not just the one running task_0.
    #
    # After fixing, these assertions should FLIP:
    # - Currently (bug): statuses[1] == "SIGTERM", statuses[2] == "SIGTERM"
    # - After fix:        statuses[1] == "COMPLETED", statuses[2] == "COMPLETED"
    for i in range(1, NUM_POOL_WORKERS):
        assert statuses[i] == "COMPLETED", (
            f"task_{i} was NOT cancelled but received SIGTERM anyway. "
            f"This is the _kill_subprocesses blast-radius bug: cancelling "
            f"task_0 killed ALL pool children.\n"
            f"All statuses: {statuses}"
        )
