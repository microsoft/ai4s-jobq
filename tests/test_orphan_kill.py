# Copyright (c) Microsoft Corporation.
# Licensed under the MIT License.
"""Tests for orphaned-process and SIGKILL escalation in run_cmd_and_log_outputs.

Verifies that:
1. Normal commands exit cleanly with correct return code.
2. Commands that spawn background children don't hang — the drain timeout
   fires, the process group is killed, and the function returns.
3. After SIGTERM, if the process doesn't exit within JOBQ_KILL_TIMEOUT,
   the process group receives SIGKILL.
"""

import asyncio
import os
import queue
import signal
import time

import pytest

from ai4s.jobq.entities import WorkerCanceled
from ai4s.jobq.work import run_cmd_and_log_outputs


def _drain_queue(q: queue.Queue) -> list[tuple]:
    """Collect all messages from a log queue."""
    msgs = []
    while True:
        try:
            msgs.append(q.get_nowait())
        except queue.Empty:
            break
    return msgs


def _ts():
    return time.strftime("%H:%M:%S")


# ── Test 1: Normal command ──────────────────────────────────────────────


async def test_normal_command_exits_cleanly():
    """A simple command exits and returns the correct code."""
    log_q: queue.Queue = queue.Queue()
    print(f"[{_ts()}] starting normal command", flush=True)

    ret = await run_cmd_and_log_outputs("echo hello && exit 0", log_q, "test-normal", cwd=None)

    print(f"[{_ts()}] returned {ret}", flush=True)
    assert ret == 0
    msgs = _drain_queue(log_q)
    texts = [m[2] for m in msgs]
    assert any("hello" in t for t in texts), f"Expected 'hello' in log output, got: {texts}"


async def test_failing_command_returns_nonzero():
    """A failing command returns the exit code."""
    log_q: queue.Queue = queue.Queue()
    ret = await run_cmd_and_log_outputs("exit 42", log_q, "test-fail", cwd=None)
    assert ret == 42


# ── Test 2: Background child with pipe inheritance ──────────────────────


async def test_background_child_does_not_hang():
    """A command that spawns a background child (inheriting pipes) must not
    hang.  The main process exits immediately; after DRAIN_TIMEOUT (5s) the
    stream readers are cancelled and the orphaned process group is killed.
    """
    log_q: queue.Queue = queue.Queue()
    # `sleep 3600 &` spawns a child that inherits stdout/stderr pipes.
    # The main shell exits right away with code 0.
    cmd = "sleep 3600 & echo 'main done'; exit 0"

    print(f"[{_ts()}] starting command with background child", flush=True)
    t0 = time.monotonic()
    ret = await run_cmd_and_log_outputs(cmd, log_q, "test-orphan", cwd=None)
    elapsed = time.monotonic() - t0
    print(f"[{_ts()}] returned {ret} after {elapsed:.1f}s", flush=True)

    assert ret == 0, f"Expected exit code 0, got {ret}"
    # Should complete within DRAIN_TIMEOUT (5s) + small margin, not hang.
    assert elapsed < 15, f"Took {elapsed:.1f}s — likely hung on orphaned pipe"

    msgs = _drain_queue(log_q)
    texts = [m[2] for m in msgs]
    assert any("main done" in t for t in texts), f"Expected 'main done' in output, got: {texts}"
    # Verify the warning about orphaned process was logged
    assert any("still open" in t or "orphan" in t.lower() for t in texts), (
        f"Expected orphan warning in log output, got: {texts}"
    )


# ── Test 3: SIGKILL escalation after SIGTERM timeout ───────────────────


async def test_sigkill_escalation_after_timeout():
    """If SIGTERM doesn't kill the process within JOBQ_KILL_TIMEOUT seconds,
    SIGKILL is sent to the process group.

    We use a command that traps SIGTERM and ignores it, then set a very short
    kill timeout (2s).  The function should still return within a few seconds.
    """
    log_q: queue.Queue = queue.Queue()
    # Trap SIGTERM and ignore it; just sleep forever.
    cmd = "trap '' TERM; echo 'trapping SIGTERM'; sleep 3600"

    # Short kill timeout for the test
    old_val = os.environ.get("JOBQ_KILL_TIMEOUT")
    os.environ["JOBQ_KILL_TIMEOUT"] = "2"
    try:
        print(f"[{_ts()}] starting SIGTERM-resistant process", flush=True)
        t0 = time.monotonic()

        async def send_sigterm_after(delay: float):
            """Simulate the shutdown signal after a short delay."""
            await asyncio.sleep(delay)
            # Send SIGUSR1 to ourselves — that's what the signal handler listens for.
            os.kill(os.getpid(), signal.SIGUSR1)

        # Run both: the command and the delayed signal sender
        sigterm_task = asyncio.create_task(send_sigterm_after(1.0))

        # The function raises WorkerCanceled after terminating the subprocess.
        with pytest.raises(WorkerCanceled):
            await run_cmd_and_log_outputs(cmd, log_q, "test-sigkill", cwd=None)

        elapsed = time.monotonic() - t0
        print(f"[{_ts()}] WorkerCanceled raised after {elapsed:.1f}s", flush=True)

        # Should complete: 1s (delay) + 2s (kill timeout) + small margin
        assert elapsed < 8, f"Took {elapsed:.1f}s — SIGKILL escalation may have failed"

        msgs = _drain_queue(log_q)
        texts = [m[2] for m in msgs]
        assert any("trapping SIGTERM" in t for t in texts), (
            f"Expected 'trapping SIGTERM' in output, got: {texts}"
        )
        assert any("SIGKILL" in t for t in texts), (
            f"Expected SIGKILL escalation warning, got: {texts}"
        )

        # Clean up the signal task
        if not sigterm_task.done():
            sigterm_task.cancel()
            try:
                await sigterm_task
            except asyncio.CancelledError:
                pass
    finally:
        if old_val is None:
            os.environ.pop("JOBQ_KILL_TIMEOUT", None)
        else:
            os.environ["JOBQ_KILL_TIMEOUT"] = old_val
