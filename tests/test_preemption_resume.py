# Copyright (c) Microsoft Corporation.
# Licensed under the MIT License.
"""Test that workers resume processing after a preemption signal (SIGTERM).

Simulates the production scenario:

1. Workers are processing tasks on Service Bus.
2. A preemption signal (SIGTERM) arrives.
3. The worker cancels running tasks and braces for imminent preemption.
4. No actual preemption happens — the ``JOBQ_PREEMPTION_TIMEOUT`` expires.
5. The worker resumes and successfully processes remaining tasks.
"""

import os
import shutil
import signal
import subprocess
import threading

import pytest

from ai4s.jobq import JobQ
from ai4s.jobq.auth import get_token_credential

NUM_TASKS = 5
TASK_DURATION_S = 2
PREEMPTION_TIMEOUT_S = 5
# Must be long enough for: first task to start (~5 s), preemption timeout,
# and remaining tasks to complete.
SUBPROCESS_TIMEOUT_S = 120


@pytest.mark.live
async def test_preemption_resume_processes_tasks(sb_namespace, sb_queue):
    """Send SIGTERM to a running worker (simulating preemption), let the
    preemption timeout expire (no actual kill), and verify the worker
    resumes and processes remaining tasks to completion.
    """
    fqns = f"{sb_namespace}.servicebus.windows.net"

    # ── Push tasks ──────────────────────────────────────────────────────
    async with (
        get_token_credential() as credential,
        JobQ.from_service_bus(sb_queue, fqns=fqns, credential=credential) as jobq,
    ):
        await jobq.clear()
        for i in range(NUM_TASKS):
            await jobq.push(
                {"cmd": f"sleep {TASK_DURATION_S} && echo task_{i}_done"},
                num_retries=0,
                reply_requested=False,
            )

    # ── Start the worker subprocess ─────────────────────────────────────
    executable = shutil.which("ai4s-jobq")
    assert executable, "ai4s-jobq CLI not found on PATH"

    queue_spec = f"sb://{sb_namespace}/{sb_queue}"
    env = {**os.environ, "JOBQ_PREEMPTION_TIMEOUT": str(PREEMPTION_TIMEOUT_S)}

    proc = subprocess.Popen(
        [
            executable,
            queue_spec,
            "worker",
            "-n",
            "1",
            # High enough so failure-shutdown is not triggered.
            "--max-consecutive-failures",
            str(NUM_TASKS + 5),
        ],
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        text=True,
        env=env,
    )

    # ── Read output, send SIGTERM when first task starts ────────────────
    output_lines: list[str] = []
    sigterm_sent = False

    def reader():
        nonlocal sigterm_sent
        assert proc.stdout is not None
        for line in proc.stdout:
            output_lines.append(line)
            if "Task starting" in line and not sigterm_sent:
                sigterm_sent = True
                proc.send_signal(signal.SIGTERM)

    reader_thread = threading.Thread(target=reader, daemon=True)
    reader_thread.start()
    reader_thread.join(timeout=SUBPROCESS_TIMEOUT_S)

    if reader_thread.is_alive():
        proc.kill()
        reader_thread.join(timeout=10)
        output = "".join(output_lines)
        pytest.fail(
            f"Worker hung after preemption — did not finish within "
            f"{SUBPROCESS_TIMEOUT_S}s.\nOutput (last 3000 chars):\n{output[-3000:]}"
        )

    returncode = proc.wait()
    output = "".join(output_lines)

    # ── Assertions ──────────────────────────────────────────────────────

    # 1. Worker received the SIGTERM and entered preemption handling
    assert "Shutdown requested" in output, (
        f"Expected shutdown requested message.\nOutput:\n{output[-3000:]}"
    )

    # 2. Worker resumed after preemption timeout
    assert "Resuming work" in output or "Continuing to launch new workers" in output, (
        f"Worker did not resume after preemption timeout.\nOutput:\n{output[-3000:]}"
    )

    # 3. All tasks were eventually processed successfully
    for i in range(NUM_TASKS):
        assert f"task_{i}_done" in output, (
            f"Task {i} was not processed after resume.\nOutput:\n{output[-3000:]}"
        )

    # 4. Worker exited cleanly (queue empty)
    assert returncode == 0, f"Worker exited with code {returncode}.\nOutput:\n{output[-3000:]}"


@pytest.mark.live
async def test_preemption_resume_with_background_process(sb_namespace, sb_queue):
    """Same scenario but tasks spawn a background process before doing real
    work.  This combines the two production issues: background children keeping
    pipes open (fixed in run_cmd_and_log_outputs) and preemption resume.
    """
    fqns = f"{sb_namespace}.servicebus.windows.net"

    async with (
        get_token_credential() as credential,
        JobQ.from_service_bus(sb_queue, fqns=fqns, credential=credential) as jobq,
    ):
        await jobq.clear()
        for i in range(NUM_TASKS):
            await jobq.push(
                # Start a background sleep (orphan child), then do real work.
                {"cmd": f"sleep 3600 & sleep {TASK_DURATION_S} && echo task_{i}_done"},
                num_retries=0,
                reply_requested=False,
            )

    executable = shutil.which("ai4s-jobq")
    assert executable, "ai4s-jobq CLI not found on PATH"

    queue_spec = f"sb://{sb_namespace}/{sb_queue}"
    env = {**os.environ, "JOBQ_PREEMPTION_TIMEOUT": str(PREEMPTION_TIMEOUT_S)}

    proc = subprocess.Popen(
        [
            executable,
            queue_spec,
            "worker",
            "-n",
            "1",
            "--max-consecutive-failures",
            str(NUM_TASKS + 5),
        ],
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        text=True,
        env=env,
    )

    output_lines: list[str] = []
    sigterm_sent = False

    def reader():
        nonlocal sigterm_sent
        assert proc.stdout is not None
        for line in proc.stdout:
            output_lines.append(line)
            if "Task starting" in line and not sigterm_sent:
                sigterm_sent = True
                proc.send_signal(signal.SIGTERM)

    reader_thread = threading.Thread(target=reader, daemon=True)
    reader_thread.start()
    reader_thread.join(timeout=SUBPROCESS_TIMEOUT_S)

    if reader_thread.is_alive():
        proc.kill()
        reader_thread.join(timeout=10)
        output = "".join(output_lines)
        pytest.fail(
            f"Worker hung after preemption with background processes — did not "
            f"finish within {SUBPROCESS_TIMEOUT_S}s.\n"
            f"Output (last 3000 chars):\n{output[-3000:]}"
        )

    returncode = proc.wait()
    output = "".join(output_lines)

    assert "Shutdown requested" in output, (
        f"Expected shutdown requested message.\nOutput:\n{output[-3000:]}"
    )
    assert "Resuming work" in output or "Continuing to launch new workers" in output, (
        f"Worker did not resume after preemption timeout.\nOutput:\n{output[-3000:]}"
    )
    for i in range(NUM_TASKS):
        assert f"task_{i}_done" in output, (
            f"Task {i} was not processed after resume.\nOutput:\n{output[-3000:]}"
        )
    assert returncode == 0, f"Worker exited with code {returncode}.\nOutput:\n{output[-3000:]}"
