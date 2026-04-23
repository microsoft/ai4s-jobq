# Copyright (c) Microsoft Corporation.
# Licensed under the MIT License.
"""Test lock renewal failure behavior.

When a Service Bus message lock is lost (for example because the lock expires or
another client completes the message), the heartbeat renewal loop detects the 404
and sets ``lock_lost_event``.  ``pull_and_execute`` should cancel the running
callback and return ``False``.

Critically, a lock-lost event is an infrastructure issue — not a task failure.
It must NOT increment the consecutive failure counter in ``launch_workers``.
"""

import asyncio
import logging

import aiohttp
import pytest

from ai4s.jobq import JobQ
from ai4s.jobq.auth import get_token_credential
from ai4s.jobq.orchestration.manager import TooManyFailuresException, launch_workers
from ai4s.jobq.work import Processor


@pytest.mark.live
async def test_lock_renewal_failure_returns_false(sb_namespace, sb_queue, caplog):
    """Patch ``renew_lock`` to return 404 after the callback starts, simulating
    a lost lock.  ``pull_and_execute`` must cancel the callback and return False.
    """
    fqns = f"{sb_namespace}.servicebus.windows.net"

    callback_started = asyncio.Event()
    callback_cancelled = asyncio.Event()

    async def slow_callback(**kwargs):
        callback_started.set()
        try:
            await asyncio.sleep(300)
        except asyncio.CancelledError:
            callback_cancelled.set()
            raise

    async with (
        get_token_credential() as credential,
        JobQ.from_service_bus(sb_queue, fqns=fqns, credential=credential) as jobq,
    ):
        await jobq.clear()
        await jobq.push({"action": "slow_task"}, num_retries=0, reply_requested=False)

        # Patch renew_lock on the REST client to simulate 404 after first renewal
        rest_client = jobq._client._rest_client
        original_renew = rest_client.renew_lock
        renew_count = 0

        async def fake_renew(location_url, **kwargs):
            nonlocal renew_count
            renew_count += 1
            # Let the first renewal succeed so the task is running
            if renew_count <= 1 or not callback_started.is_set():
                return await original_renew(location_url, **kwargs)
            # Subsequent renewals fail with 404 (lock lost)
            raise aiohttp.ClientResponseError(
                request_info=aiohttp.RequestInfo(
                    url=location_url,
                    method="POST",
                    headers={},
                    real_url=location_url,
                ),
                history=(),
                status=404,
                message="Not Found",
            )

        rest_client.renew_lock = fake_renew

        with caplog.at_level(logging.DEBUG):
            result = await jobq.pull_and_execute(
                slow_callback,
                with_heartbeat=True,
            )

    # ── Assertions ──────────────────────────────────────────────────────
    assert result is False, f"Expected pull_and_execute to return False, got {result}"
    assert callback_cancelled.is_set(), "Expected callback to be cancelled when lock was lost"

    assert any(
        "Lock lost" in r.message and "404" in r.message for r in caplog.records
    ), "Expected lock-lost 404 log message"
    assert any(
        "Lock lost for task" in r.message and "abandoning without settlement" in r.message
        for r in caplog.records
    ), "Expected task-level lock-lost log message"


class SlowProcessor(Processor):
    """Processor that sleeps until cancelled, tracking how many times it was called."""

    def __init__(self):
        self.call_count = 0
        super().__init__()

    async def __call__(self, **kwargs):
        self.call_count += 1
        try:
            await asyncio.sleep(300)
        except asyncio.CancelledError:
            raise


@pytest.mark.live
async def test_lock_loss_should_not_count_as_consecutive_failure(sb_namespace, sb_queue):
    """BUG: lock-lost events currently increment num_consecutive_failures in
    launch_workers.  This means a burst of lock renewals failing (for example, a
    transient network issue) can trigger TooManyFailuresException and shut down
    all workers — even though no task actually failed.

    Lock loss is an infrastructure event, not a task failure, and should NOT
    count toward the consecutive failure threshold.

    This test pushes several tasks, patches renew_lock to always 404 (simulating
    persistent lock loss), and runs launch_workers with max_consecutive_failures=2.
    If lock-lost events are correctly excluded from the failure counter, the
    workers should keep retrying until the queue is empty — NOT raise
    TooManyFailuresException.
    """
    fqns = f"{sb_namespace}.servicebus.windows.net"
    num_tasks = 5
    max_consecutive_failures = 2

    async with (
        get_token_credential() as credential,
        JobQ.from_service_bus(sb_queue, fqns=fqns, credential=credential) as jobq,
    ):
        await jobq.clear()
        for i in range(num_tasks):
            await jobq.push({"task_index": i}, num_retries=0, reply_requested=False)

        # Patch renew_lock to always fail with 404
        rest_client = jobq._client._rest_client
        original_renew = rest_client.renew_lock

        async def always_fail_renew(location_url, **kwargs):
            raise aiohttp.ClientResponseError(
                request_info=aiohttp.RequestInfo(
                    url=location_url,
                    method="POST",
                    headers={},
                    real_url=location_url,
                ),
                history=(),
                status=404,
                message="Not Found",
            )

        rest_client.renew_lock = always_fail_renew

        proc = SlowProcessor()

        # BUG: This SHOULD NOT raise TooManyFailuresException, because lock-lost
        # is not a task failure.  But it currently does.
        with pytest.raises(TooManyFailuresException):
            await launch_workers(
                jobq,
                proc,
                num_workers=1,
                max_consecutive_failures=max_consecutive_failures,
                show_progress=False,
            )

        # If the bug is fixed, remove the pytest.raises above and instead assert:
        #   - Workers drained the queue without raising TooManyFailuresException
        #   - proc.call_count == num_tasks
        pytest.xfail(
            "BUG: lock-lost events are incorrectly counted as consecutive failures. "
            "See https://github.com/..../issues/XXX"
        )
