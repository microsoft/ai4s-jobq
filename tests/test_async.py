import asyncio
from datetime import timedelta

import pytest

from ai4s.jobq.entities import EmptyQueue, LockLostError
from ai4s.jobq.jobq import JobQ, _is_async_callable


@pytest.mark.asyncio
async def test_async(mocker, async_queue, azurite_connstr):
    """
    check whether we can queue a command and execute it in a worker
    """
    commands = []

    async def callback(cmd: str):
        commands.append(cmd)

    await async_queue.push("echo hello world")

    async with JobQ.from_connection_string("jobs", connection_string=azurite_connstr) as q_worker:
        success0 = await q_worker.pull_and_execute(callback)
        with pytest.raises(EmptyQueue):
            await q_worker.pull_and_execute(callback)

    assert success0
    assert commands == ["echo hello world"]


@pytest.mark.asyncio
async def test_heartbeat_lock_lost(async_queue, azurite_connstr):
    """When the heartbeat detects that the lock is lost, the task should be cancelled
    and the worker should return False without trying to settle the message."""
    callback_started = asyncio.Event()
    callback_cancelled = asyncio.Event()

    async def callback(cmd: str):
        callback_started.set()
        try:
            # Long sleep that should be cancelled when the lock is lost.
            await asyncio.sleep(3600)
        except asyncio.CancelledError:
            callback_cancelled.set()
            raise

    await async_queue.push("echo hello world")

    async with JobQ.from_connection_string("jobs", connection_string=azurite_connstr) as q_worker:
        # Access the underlying storage queue client so we can sabotage the message.
        backend = q_worker._client
        assert backend.queue_client is not None

        original_update = backend.queue_client.update_message

        call_count = 0

        async def sabotaged_update(message, *args, **kwargs):
            nonlocal call_count
            call_count += 1
            if call_count >= 2:
                # Corrupt the pop receipt so the real Azure API rejects it.
                kwargs["pop_receipt"] = "invalid-pop-receipt"
            return await original_update(message, *args, **kwargs)

        backend.queue_client.update_message = sabotaged_update

        with pytest.raises(LockLostError):
            await q_worker.pull_and_execute(
                callback, visibility_timeout=timedelta(seconds=2), with_heartbeat=True
            )

    assert callback_cancelled.is_set(), "Callback should have been cancelled"

    # The message should NOT have been deleted or deadlettered — it must still
    # be in the queue so another worker can pick it up.  Wait for the
    # visibility timeout to expire so it becomes visible again.
    await asyncio.sleep(3)
    async with JobQ.from_connection_string("jobs", connection_string=azurite_connstr) as q2:
        msgs = await q2.peek(1)
        assert len(msgs) >= 1, "Message should still be in the queue"
    """
    check whether we can execute a long-running command by sending continuous heartbeat updates
    """

    job_duration = 10

    async def callback(cmd: str):
        await asyncio.sleep(job_duration)

    await async_queue.push("echo hello world")

    async with JobQ.from_connection_string("jobs", connection_string=azurite_connstr) as q_worker:
        # set minimum visibility timeout, which should cause expiry
        success = await q_worker.pull_and_execute(
            callback, visibility_timeout=timedelta(seconds=2), with_heartbeat=True
        )
    assert success


@pytest.mark.asyncio
async def test_is_aio_callable():
    async def async_fn():
        pass

    def sync_fn():
        pass

    class A:
        async def __call__(self):
            pass

    class B:
        def __call__(self):
            pass

    assert _is_async_callable(async_fn)
    assert not _is_async_callable(sync_fn)
    assert _is_async_callable(A())
    assert not _is_async_callable(B())
