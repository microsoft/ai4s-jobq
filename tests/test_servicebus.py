from collections import Counter
from uuid import uuid4

import pytest

from ai4s.jobq import JobQ, Processor, WorkSpecification
from ai4s.jobq.auth import get_token_credential
from ai4s.jobq.orchestration.manager import batch_enqueue, launch_workers


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


@pytest.mark.live
@pytest.mark.asyncio
async def test_servicebus_stress(sb_namespace, sb_queue):
    n_seeds, n_tasks_per_seed = 10, 20

    expected = set()

    class Work(WorkSpecification, Processor):
        def __init__(self):
            self.done = Counter()
            super().__init__()

        async def task_seeds(self):
            for item in [f"seed {_}" for _ in range(n_seeds)]:
                yield item

        async def list_tasks(self, seed: str, force: bool = False):
            for i in range(n_tasks_per_seed):
                cmd = f"{seed} - task {i}"
                yield cmd
                expected.add(cmd)

        async def __call__(self, cmd):
            self.done[cmd] += 1
            return f"processed {cmd}"

    async with Work() as work:
        async with get_token_credential() as credential:
            async with JobQ.from_service_bus(
                sb_queue, fqns=f"{sb_namespace}.servicebus.windows.net", credential=credential
            ) as jobq:
                await jobq.clear()

                await batch_enqueue(jobq, work)
                await launch_workers(jobq, work, num_workers=20)

    for v in work.done.values():
        assert v == 1

    assert len(work.done) == n_seeds * n_tasks_per_seed
    assert set(work.done.keys()) == expected
