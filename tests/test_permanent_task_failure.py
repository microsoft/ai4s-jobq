# Copyright (c) Microsoft Corporation.
# Licensed under the MIT License.
"""Tests for the ``PermanentTaskFailure`` short-circuit in the consumer loop.

Contract: when a task callback raises :class:`PermanentTaskFailure`, the
consumer loop must skip the remaining retry budget and dead-letter the
message immediately.  This avoids burning ``num_retries`` worker invocations
on a deterministic failure (for example, a payload routed to the wrong
queue).
"""

from __future__ import annotations

import pytest

from ai4s.jobq import EmptyQueue, PermanentTaskFailure


async def _peek_dlq(async_queue) -> list:
    """Return all messages currently in the dead-letter queue."""
    backend = async_queue._client
    assert backend.dead_letter_queue_client is not None
    return await backend.dead_letter_queue_client.peek_messages(max_messages=32)


async def _clear_dlq(async_queue) -> None:
    """Drain the dead-letter queue (the ``async_queue`` fixture only clears the main queue)."""
    backend = async_queue._client
    assert backend.dead_letter_queue_client is not None
    await backend.dead_letter_queue_client.clear_messages()


async def test_permanent_failure_short_circuits_retries(async_queue):
    """A callback raising ``PermanentTaskFailure`` must not be retried."""
    await _clear_dlq(async_queue)
    await async_queue.push({"value": 1}, num_retries=5)

    invocations = 0

    async def callback(value):
        nonlocal invocations
        invocations += 1
        raise PermanentTaskFailure(f"wrong queue for value={value}")

    result = await async_queue.pull_and_execute(callback)

    assert result is False
    assert invocations == 1, "callback must run exactly once before dead-lettering"

    # Main queue is now empty — message was deleted, not requeued.
    with pytest.raises(EmptyQueue):
        await async_queue.pull_and_execute(callback)
    assert invocations == 1, "no retries should have been issued"

    # Dead-letter queue should contain exactly one message.
    dlq_messages = await _peek_dlq(async_queue)
    assert len(dlq_messages) == 1


async def test_regular_exception_still_retries(async_queue):
    """Sanity check: a generic ``RuntimeError`` keeps the existing retry path."""
    await _clear_dlq(async_queue)
    await async_queue.push({"value": 1}, num_retries=2)

    invocations = 0

    async def callback(value):
        nonlocal invocations
        invocations += 1
        raise RuntimeError("transient")

    # First attempt: fails, message is requeued with one retry left.
    result = await async_queue.pull_and_execute(callback)
    assert result is False
    assert invocations == 1

    # DLQ stays empty until retries are exhausted.
    assert await _peek_dlq(async_queue) == []
