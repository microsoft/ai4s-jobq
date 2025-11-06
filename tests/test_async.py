import asyncio
from datetime import timedelta

import pytest

from ai4s.jobq.entities import EmptyQueue
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
async def test_heartbeat(async_queue, azurite_connstr):
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
