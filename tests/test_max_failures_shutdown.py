# Copyright (c) Microsoft Corporation.
# Licensed under the MIT License.
"""Test that the CLI worker process shuts down when max consecutive failures is reached.

Reproduces the production scenario: push tasks that always fail to a Service Bus
queue, then run ``ai4s-jobq worker`` as a subprocess with 8 parallel workers.
The process must exit (not hang) once the failure threshold is exceeded.
"""

import asyncio
import shutil
import subprocess
import time

import pytest

from ai4s.jobq import JobQ
from ai4s.jobq.auth import get_token_credential


def _ts():
    return time.strftime("%H:%M:%S")


NUM_WORKERS = 8
MAX_CONSECUTIVE_FAILURES = 3
# Enough tasks so every worker can exceed the threshold before the queue empties.
NUM_TASKS = NUM_WORKERS * (MAX_CONSECUTIVE_FAILURES + 2)
# The subprocess must exit within this many seconds; if it doesn't, it's hanging.
SUBPROCESS_TIMEOUT = 120


async def _push_failing_tasks(sb_namespace, sb_queue, cmd):
    """Push NUM_TASKS copies of *cmd* to the Service Bus queue."""
    async with (
        get_token_credential() as credential,
        JobQ.from_service_bus(
            sb_queue,
            fqns=f"{sb_namespace}.servicebus.windows.net",
            credential=credential,
        ) as jobq,
    ):
        await jobq.clear()
        for _ in range(NUM_TASKS):
            await jobq.push(cmd, num_retries=0, reply_requested=False)


def _run_worker(sb_namespace, sb_queue):
    """Run ai4s-jobq worker as a subprocess and return the CompletedProcess."""
    executable = shutil.which("ai4s-jobq")
    assert executable, "ai4s-jobq CLI not found on PATH"

    queue_spec = f"sb://{sb_namespace}/{sb_queue}"
    cmd = [
        executable,
        queue_spec,
        "worker",
        "-n",
        str(NUM_WORKERS),
        "--max-consecutive-failures",
        str(MAX_CONSECUTIVE_FAILURES),
        "--no-heartbeat",
    ]

    return subprocess.run(
        cmd,
        check=False,
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        text=True,
        timeout=SUBPROCESS_TIMEOUT,
    )


def _assert_clean_shutdown(output):
    """Check that the worker output contains the expected shutdown messages."""
    assert "Maximum number of consecutive failures" in output, (
        f"Expected max-failures log message in output.\n{output[-2000:]}"
    )
    assert "Too many consecutive failures" in output, (
        f"Expected cancellation log message in output.\n{output[-2000:]}"
    )
    assert "Stopping workforce monitor" in output, (
        f"Expected workforce monitor stop message in output.\n{output[-2000:]}"
    )


@pytest.mark.live
async def test_max_failures_shutdown_servicebus_subprocess(sb_namespace, sb_queue):
    """Push always-failing tasks to Service Bus, run ``ai4s-jobq worker`` as a
    real subprocess, and verify it exits cleanly instead of hanging.
    """
    print(f"[{_ts()}] pushing failing tasks (exit 1)", flush=True)
    await _push_failing_tasks(sb_namespace, sb_queue, "exit 1")

    print(f"[{_ts()}] starting worker subprocess", flush=True)
    result = await asyncio.to_thread(_run_worker, sb_namespace, sb_queue)
    print(f"[{_ts()}] worker exited with code {result.returncode}", flush=True)
    _assert_clean_shutdown(result.stdout)
    print(f"[{_ts()}] test_max_failures_shutdown_servicebus_subprocess PASSED", flush=True)


@pytest.mark.live
async def test_max_failures_shutdown_with_background_process(sb_namespace, sb_queue):
    """Same scenario but with tasks that spawn a background thread/process
    before failing — reproduces hangs caused by orphaned child processes
    keeping pipes open.
    """
    # Start a long-running background process, then exit with failure.
    # The background sleep inherits stdout/stderr file descriptors from bash,
    # which can block the pipe reads in run_cmd_and_log_outputs.
    print(f"[{_ts()}] pushing failing tasks (sleep 3600 & exit 1)", flush=True)
    await _push_failing_tasks(sb_namespace, sb_queue, "sleep 3600 & exit 1")

    print(f"[{_ts()}] starting worker subprocess", flush=True)
    result = await asyncio.to_thread(_run_worker, sb_namespace, sb_queue)
    print(f"[{_ts()}] worker exited with code {result.returncode}", flush=True)
    _assert_clean_shutdown(result.stdout)
    print(f"[{_ts()}] test_max_failures_shutdown_with_background_process PASSED", flush=True)
