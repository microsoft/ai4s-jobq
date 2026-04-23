# Copyright (c) Microsoft Corporation.
# Licensed under the MIT License.
"""Test lock renewal failure behavior.

When a Service Bus message lock is lost (for example because the lock expires or
another client completes the message), the heartbeat renewal loop detects the 404
and sets ``lock_lost_event``.  ``pull_and_execute`` should cancel the running
callback and raise ``LockLostError``.

Critically, a lock-lost event is an infrastructure issue — not a task failure.
It must NOT increment the consecutive failure counter in ``launch_workers``.
"""

import asyncio
import logging
import time

import aiohttp
import pytest

from ai4s.jobq import JobQ
from ai4s.jobq.auth import get_token_credential
from ai4s.jobq.entities import LockLostError
from ai4s.jobq.orchestration.manager import launch_workers
from ai4s.jobq.work import Processor


def _ts():
    return time.strftime("%H:%M:%S")



@pytest.mark.live
async def test_lock_renewal_failure_raises_lock_lost(sb_namespace, sb_queue, caplog):
    """Patch ``renew_lock`` to return 404 after the callback starts, simulating
    a lost lock.  ``pull_and_execute`` must cancel the callback and raise
    ``LockLostError``.
    """
    fqns = f"{sb_namespace}.servicebus.windows.net"

    callback_started = asyncio.Event()
    callback_cancelled = asyncio.Event()

    async def slow_callback(**kwargs):
        print(f"  [{_ts()}] callback started", flush=True)
        callback_started.set()
        try:
            await asyncio.sleep(300)
        except asyncio.CancelledError:
            print(f"  [{_ts()}] callback cancelled", flush=True)
            callback_cancelled.set()
            raise

    print(f"[{_ts()}] connecting to {sb_namespace}/{sb_queue}", flush=True)
    async with (
        get_token_credential() as credential,
        JobQ.from_service_bus(sb_queue, fqns=fqns, credential=credential) as jobq,
    ):
        print(f"[{_ts()}] clearing queue and pushing task", flush=True)
        await jobq.clear()
        await jobq.push({"action": "slow_task"}, num_retries=0, reply_requested=False)

        # Patch renew_lock on the REST client to simulate 404 after first renewal
        rest_client = jobq._client._rest_client
        original_renew = rest_client.renew_lock
        renew_count = 0

        async def fake_renew(location_url, **kwargs):
            nonlocal renew_count
            renew_count += 1
            print(f"  [{_ts()}] fake_renew called (#{renew_count})", flush=True)
            # Let the first renewal succeed so the task is running
            if renew_count <= 1 or not callback_started.is_set():
                return await original_renew(location_url, **kwargs)
            # Subsequent renewals fail with 404 (lock lost)
            print(f"  [{_ts()}] fake_renew raising 404", flush=True)
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

        print(f"[{_ts()}] calling pull_and_execute (expecting LockLostError)", flush=True)
        with caplog.at_level(logging.DEBUG), pytest.raises(LockLostError):
            await jobq.pull_and_execute(
                slow_callback,
                with_heartbeat=True,
            )
        print(f"[{_ts()}] LockLostError raised as expected", flush=True)

    # ── Assertions ──────────────────────────────────────────────────────
    assert callback_cancelled.is_set(), "Expected callback to be cancelled when lock was lost"

    assert any(
        "Lock lost" in r.message and "404" in r.message for r in caplog.records
    ), "Expected lock-lost 404 log message"
    assert any(
        "Lock lost for task" in r.message and "abandoning without settlement" in r.message
        for r in caplog.records
    ), "Expected task-level lock-lost log message"
    print(f"[{_ts()}] test_lock_renewal_failure_raises_lock_lost PASSED", flush=True)


class SlowProcessor(Processor):
    """Processor that sleeps until cancelled, tracking how many times it was called."""

    def __init__(self):
        self.call_count = 0
        super().__init__()

    async def __call__(self, **kwargs):
        self.call_count += 1
        print(f"  [{_ts()}] SlowProcessor call #{self.call_count}", flush=True)
        try:
            await asyncio.sleep(300)
        except asyncio.CancelledError:
            print(f"  [{_ts()}] SlowProcessor call #{self.call_count} cancelled", flush=True)
            raise


@pytest.mark.live
async def test_lock_loss_should_not_count_as_consecutive_failure(sb_namespace, sb_queue):
    """Lock-lost events must NOT increment num_consecutive_failures in
    launch_workers.  A burst of lock renewals failing (for example, a transient
    network issue) must not trigger TooManyFailuresException and shut down all
    workers — because no task actually failed.

    This test pushes a task, patches renew_lock to always 404 (simulating
    persistent lock loss), and runs launch_workers with max_consecutive_failures=2.
    Since lock-lost messages are never settled they return to the queue, so we
    use a timeout to stop the test after enough iterations.  The key assertion
    is that TooManyFailuresException is never raised, and that the processor
    was called more times than max_consecutive_failures would allow if lock
    loss were counted as a failure.
    """
    fqns = f"{sb_namespace}.servicebus.windows.net"
    max_consecutive_failures = 2
    # We need enough iterations to prove lock loss doesn't increment the
    # failure counter.  If it did, we'd get TooManyFailuresException after
    # max_consecutive_failures + 1 = 3 calls.  We run for more than that.
    min_expected_calls = max_consecutive_failures + 2

    print(f"[{_ts()}] connecting to {sb_namespace}/{sb_queue}", flush=True)
    async with (
        get_token_credential() as credential,
        JobQ.from_service_bus(sb_queue, fqns=fqns, credential=credential) as jobq,
    ):
        print(f"[{_ts()}] clearing queue and pushing 1 task", flush=True)
        await jobq.clear()
        await jobq.push({"task_index": 0}, num_retries=0, reply_requested=False)

        # Patch renew_lock to always fail with 404
        rest_client = jobq._client._rest_client

        async def always_fail_renew(location_url, **kwargs):
            print(f"  [{_ts()}] always_fail_renew: raising 404", flush=True)
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

        # Lock-lost events are NOT task failures — this must NOT raise
        # TooManyFailuresException.  Since lock-lost messages return to
        # the queue, launch_workers will never drain it.  We use a timeout
        # to stop after enough iterations to prove correctness.
        print(f"[{_ts()}] calling launch_workers (will timeout after enough iterations)", flush=True)
        try:
            await asyncio.wait_for(
                launch_workers(
                    jobq,
                    proc,
                    num_workers=1,
                    max_consecutive_failures=max_consecutive_failures,
                    show_progress=False,
                ),
                timeout=min_expected_calls * 35,  # ~30s per lock cycle + margin
            )
        except TimeoutError:
            # Expected — the queue never drains because messages return after lock loss
            print(f"[{_ts()}] timed out as expected after {proc.call_count} calls", flush=True)

        print(f"[{_ts()}] proc.call_count = {proc.call_count}", flush=True)
        assert proc.call_count >= min_expected_calls, (
            f"Expected at least {min_expected_calls} calls to prove lock loss "
            f"doesn't count as failure, but only got {proc.call_count}. "
            f"If lock loss were counted, TooManyFailuresException would have "
            f"fired after {max_consecutive_failures + 1} calls."
        )

        # Clean up — drain the queue so it doesn't interfere with other tests
        print(f"[{_ts()}] clearing queue", flush=True)
        await jobq.clear()
    print(f"[{_ts()}] test_lock_loss_should_not_count_as_consecutive_failure PASSED", flush=True)
