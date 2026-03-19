# Copyright (c) Microsoft Corporation.
# Licensed under the MIT License.
"""Live test: confirm dead-lettered messages are NOT re-delivered to regular receivers."""

import asyncio

import pytest

from ai4s.jobq.auth import get_token_credential
from ai4s.jobq.backend.servicebus_rest import ServiceBusRestBackend, ServiceBusRestClient
from ai4s.jobq.entities import EmptyQueue, Task


@pytest.mark.live
@pytest.mark.asyncio
async def test_deadlettered_message_is_not_redelivered(sb_namespace, sb_queue):
    """
    1. Clear the queue.
    2. Send one message.
    3. Peek-lock it, then dead-letter it via the envelope.
    4. Attempt to receive again from the main queue → must raise EmptyQueue.
    5. Verify DLQ count increased via the admin client.
    """
    fqns = f"{sb_namespace}.servicebus.windows.net"
    credential = get_token_credential()

    async with ServiceBusRestBackend(
        queue_name=sb_queue, fqns=fqns, credential=credential, exist_ok=True
    ) as backend:
        client: ServiceBusRestClient = backend._rest_client  # type: ignore[assignment]

        # ── 0. snapshot DLQ count before test ────────────────────────────
        async with backend._get_admin_client() as admin:
            props_before = await admin.get_queue_runtime_properties(sb_queue)
            dlq_count_before = props_before.dead_letter_message_count

        # ── 1. clear the main queue ──────────────────────────────────────
        await backend.clear()

        # ── 2. send one task ─────────────────────────────────────────────
        task = Task(kwargs={"payload": "dlq-test"}, num_retries=0)
        await client.send_message(task.serialize())

        # ── 3. peek-lock, then dead-letter ───────────────────────────────
        msg = await client.peek_lock_message(timeout=10)
        assert msg.body == task.serialize()

        await client.deadletter_message(
            msg.location_url,
            sequence_number=msg.sequence_number,
            lock_token=msg.lock_token,
            reason="live-test",
        )

        # ── 4. main queue must be empty now ──────────────────────────────
        with pytest.raises(EmptyQueue):
            await client.peek_lock_message(timeout=5)

        # ── 5. DLQ count must have increased ─────────────────────────────
        # Service Bus counters are eventually consistent; poll briefly.
        for _ in range(10):
            async with backend._get_admin_client() as admin:
                props_after = await admin.get_queue_runtime_properties(sb_queue)
            if props_after.dead_letter_message_count > dlq_count_before:
                break
            await asyncio.sleep(1)

        assert props_after.dead_letter_message_count > dlq_count_before, (
            f"DLQ count did not increase: before={dlq_count_before}, "
            f"after={props_after.dead_letter_message_count}"
        )
