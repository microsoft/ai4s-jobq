from uuid import uuid4

import pytest

from ai4s.jobq import JobQ
from ai4s.jobq.auth import get_token_credential


@pytest.mark.live
@pytest.mark.asyncio
async def test_servicebus(sb_namespace, sb_queue):
    async def callback(cmd):
        return f"processed {cmd}"

    async with JobQ.from_service_bus(
        sb_queue, fqns=f"{sb_namespace}.servicebus.windows.net", credential=get_token_credential()
    ) as jobq:
        print("CLEARING.....", jobq.full_name)
        await jobq.clear()

        async with jobq.get_worker() as worker_interface:
            print("SENDING.....")
            await jobq.push(f"test {uuid4()}", reply_requested=False, worker=worker_interface)

            msgs = await jobq._client.peek(n=10)
            assert len(msgs) == 1

            print("PEEKING.....")
            print("RECEIVING.....")
            await jobq.pull_and_execute(callback, worker=worker_interface)
            print("DONE.")
