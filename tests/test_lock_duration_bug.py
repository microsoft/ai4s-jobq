# Copyright (c) Microsoft Corporation.
# Licensed under the MIT License.
"""Demonstrate that lock_duration drops to 30 s after the first renewal.

BUG
---
The Service Bus REST API renewal response (POST to the message location URL)
returns **empty** ``BrokerProperties`` — no ``LockedUntilUtc`` field at all.
``_parse_lock_duration`` therefore falls back to its hardcoded **30 s** default.

After the first successful renewal the ``_renew_loop`` overwrites its
``lock_duration`` variable with this bogus value, which causes three
cascading problems:

1. **Renewal interval halves** (``lock_duration / 2``): 150 s → 15 s.
   Harmless but wasteful.
2. **Per-request timeout shrinks** (``max(lock_duration / 3, 5)``):
   100 s → 10 s.  A slow but successful renewal may now time out.
3. **False lock-loss detection**: the check
   ``if elapsed > lock_duration`` fires at 30 s instead of 300 s.
   A single transient failure with retries totalling > 30 s is wrongly
   interpreted as lock expiry, setting ``lock_lost_event`` and
   cancelling the running task — even though the real 5-minute lock is
   still perfectly valid.

FIX
---
Do **not** overwrite ``lock_duration`` from ``renew_lock()``'s return
value.  The initial ``lock_duration_seconds`` obtained from the peek-lock
response (which includes ``LockedUntilUtc``) is correct and must be used
for the entire lifetime of the renewal loop.  Concretely, remove or guard
the three lines in ``_renew_loop`` that reassign ``lock_duration``,
``message.lock_duration_seconds``, and ``per_request_timeout`` after a
successful renewal.
"""

import asyncio
import json
import time

import pytest

from ai4s.jobq import JobQ
from ai4s.jobq.auth import get_token_credential


def _ts():
    return time.strftime("%H:%M:%S")


@pytest.mark.live
async def test_lock_duration_drops_after_renewal(sb_namespace, sb_queue):
    """Show that the reported lock_duration collapses from ~300 s to 30 s
    after a single renewal, proving the BrokerProperties-is-empty bug.
    """
    fqns = f"{sb_namespace}.servicebus.windows.net"

    print(f"[{_ts()}] connecting to {sb_namespace}/{sb_queue}", flush=True)
    async with (
        get_token_credential() as credential,
        JobQ.from_service_bus(sb_queue, fqns=fqns, credential=credential) as jobq,
    ):
        print(f"[{_ts()}] clearing queue and pushing task", flush=True)
        await jobq.clear()
        await jobq.push({"test": "lock_duration_bug"}, num_retries=0, reply_requested=False)

        rest_client = jobq._client._rest_client
        assert rest_client is not None

        print(f"[{_ts()}] peek-locking message", flush=True)
        msg = await rest_client.peek_lock_message(timeout=10)

        # ── Initial lock duration from peek-lock ────────────────────────
        initial_duration = msg.lock_duration_seconds
        print(f"[{_ts()}] initial lock_duration = {initial_duration:.1f}s", flush=True)
        assert initial_duration > 31, (
            f"Queue lock duration should be > 31 s (got {initial_duration:.1f} s). "
            f"The bug causes a drop to exactly 30 s — we need a queue "
            f"configured with a longer lock to demonstrate the difference."
        )

        # ── Renew the lock once ─────────────────────────────────────────
        print(f"[{_ts()}] renewing lock", flush=True)
        await asyncio.sleep(2)  # small delay so LockedUntilUtc would differ
        renewed_duration = await rest_client.renew_lock(msg.location_url)
        print(f"[{_ts()}] renewed lock_duration = {renewed_duration:.1f}s", flush=True)

        # ── Verify the raw response is empty ────────────────────────────
        # Do a manual POST to inspect headers directly
        resp = await rest_client._request("POST", msg.location_url, max_retries=1)
        broker_props = json.loads(resp.headers.get("BrokerProperties", "{}"))
        resp.close()

        assert "LockedUntilUtc" not in broker_props, (
            "If this fails, Azure has started returning LockedUntilUtc on "
            "renewal — the fallback bug may no longer apply."
        )

        # ── This is the bug ─────────────────────────────────────────────
        assert renewed_duration == 30.0, (
            f"Expected _parse_lock_duration to fall back to 30 s (got {renewed_duration:.1f} s) "
            f"because BrokerProperties is empty on renewal."
        )
        assert renewed_duration < initial_duration, (
            f"BUG: After one renewal, lock_duration dropped from "
            f"{initial_duration:.0f} s to {renewed_duration:.0f} s. "
            f"The renewal loop will now use a 30 s threshold for lock-loss "
            f"detection instead of the real {initial_duration:.0f} s."
        )

        # ── Clean up ────────────────────────────────────────────────────
        await rest_client.complete_message(msg.location_url)
