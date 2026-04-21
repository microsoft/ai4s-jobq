# Copyright (c) Microsoft Corporation.
# Licensed under the MIT License.
"""Live test: verify that a failed task does not cause message buildup in Service Bus.

The current ``replace()`` implementation sends a NEW message without deleting the old
one, which causes the queue to grow with every failure.  This test reproduces that
behavior so we can validate the fix.
"""

import asyncio
from datetime import timedelta

import pytest

from ai4s.jobq.auth import get_token_credential
from ai4s.jobq.backend.servicebus_rest import ServiceBusRestBackend
from ai4s.jobq.entities import Task


@pytest.mark.live
@pytest.mark.asyncio
async def test_replace_does_not_duplicate_message(sb_namespace, sb_queue):
    """Push one task, fail it so replace() is triggered, verify queue size stays 1.

    Steps:
        1. Clear the queue.
        2. Send one task with num_retries=10.
        3. Peek-lock the message, simulate a failure by calling envelope.replace()
           with a decremented retry count, then abandon (don't complete) the original.
        4. Wait for things to settle.
        5. Assert the queue has exactly 1 active message — not 2.
    """
    fqns = f"{sb_namespace}.servicebus.windows.net"
    credential = get_token_credential()

    async with ServiceBusRestBackend(
        queue_name=sb_queue, fqns=fqns, credential=credential, exist_ok=True
    ) as backend:
        # ── 1. clear ────────────────────────────────────────────────────
        await backend.clear()

        # ── 2. push one task ────────────────────────────────────────────
        task = Task(kwargs={"cmd": "echo fail-test"}, num_retries=10)
        await backend.push(task)

        # Verify starting count
        count_before = await _active_message_count(backend, sb_queue)
        assert count_before == 1, f"Expected 1 message after push, got {count_before}"

        # ── 3. peek-lock, trigger replace, then abandon ─────────────────
        from dataclasses import replace as dc_replace

        async with backend.receive_message(
            visibility_timeout=timedelta(seconds=60),
            with_heartbeat=False,
        ) as envelope:
            # Simulate the retry path in jobq.py pull_and_execute:
            # task fails → replace(task, num_retries=num_retries-1)
            retry_task = dc_replace(envelope.task, num_retries=envelope.task.num_retries - 1)
            await envelope.replace(retry_task)

            # Now abandon the original (this is what happens when the worker
            # moves on — the message becomes visible again after lock expiry)
            await envelope.abandon()

        # ── 4. wait for things to settle ────────────────────────────────
        # Service Bus counters are eventually consistent.
        await asyncio.sleep(5)

        # ── 5. verify no message buildup ────────────────────────────────
        count_after = await _active_message_count(backend, sb_queue)
        assert count_after == 1, (
            f"Message buildup detected! Expected 1 active message, got {count_after}. "
            "replace() likely sent a new message without removing the old one."
        )


@pytest.mark.live
@pytest.mark.asyncio
async def test_requeue_does_not_duplicate_message(sb_namespace, sb_queue):
    """Push one task, simulate preemption via requeue(), verify queue size stays 1.

    Steps:
        1. Clear the queue.
        2. Send one task.
        3. Peek-lock the message, then requeue (simulating WorkerCanceled / preemption).
        4. Wait for things to settle.
        5. Assert the queue has exactly 1 active message.
    """
    fqns = f"{sb_namespace}.servicebus.windows.net"
    credential = get_token_credential()

    async with ServiceBusRestBackend(
        queue_name=sb_queue, fqns=fqns, credential=credential, exist_ok=True
    ) as backend:
        # ── 1. clear ────────────────────────────────────────────────────
        await backend.clear()

        # ── 2. push one task ────────────────────────────────────────────
        task = Task(kwargs={"cmd": "echo preempt-test"}, num_retries=5)
        await backend.push(task)

        count_before = await _active_message_count(backend, sb_queue)
        assert count_before == 1, f"Expected 1 message after push, got {count_before}"

        # ── 3. peek-lock, then requeue (preemption path) ────────────────
        async with backend.receive_message(
            visibility_timeout=timedelta(seconds=60),
            with_heartbeat=False,
        ) as envelope:
            # Simulate the preemption path in jobq.py pull_and_execute:
            # WorkerCanceled is raised → envelope.requeue()
            await envelope.requeue()

        # ── 4. wait for things to settle ────────────────────────────────
        await asyncio.sleep(5)

        # ── 5. verify no message buildup ────────────────────────────────
        count_after = await _active_message_count(backend, sb_queue)
        assert count_after == 1, (
            f"Message buildup detected! Expected 1 active message, got {count_after}. "
            "requeue() should unlock the message, not create a new one."
        )


async def _active_message_count(backend: ServiceBusRestBackend, queue_name: str) -> int:
    """Poll active message count (eventually consistent)."""
    for _ in range(10):
        async with backend._get_admin_client() as admin:
            props = await admin.get_queue_runtime_properties(queue_name)
            if props is not None:
                return props.active_message_count  # type: ignore[return-value]
        await asyncio.sleep(1)
    raise RuntimeError("Could not get queue runtime properties")
