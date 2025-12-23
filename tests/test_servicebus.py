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

        async with jobq.get_worker_interface() as worker_interface:
            print("SENDING.....")
            await jobq.push(
                f"test {uuid4()}", reply_requested=False, worker_interface=worker_interface
            )

            msgs = await jobq._client.peek(n=10)
            assert len(msgs) == 1

            print("PEEKING.....")
            print("RECEIVING.....")
            await jobq.pull_and_execute(callback, worker_interface=worker_interface)
            print("DONE.")


@pytest.mark.live
@pytest.mark.asyncio
async def test_servicebus_stress(sb_namespace, sb_queue):
    n_seeds, n_tasks_per_seed = 10, 200

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


@pytest.mark.live
@pytest.mark.asyncio
async def test_servicebus_stress_mp(sb_namespace, sb_queue):
    """
    Run Servicebus test with multiple worker processes, not co-routines.
    """

    import shutil
    import subprocess
    from concurrent.futures import ProcessPoolExecutor, as_completed
    from functools import partial

    n_seeds, n_tasks_per_seed = 10, 200

    magic_str = uuid4().hex

    expected = set()

    class Work(WorkSpecification):
        def __init__(self):
            super().__init__()

        async def task_seeds(self):
            for item in [f"seed {_}" for _ in range(n_seeds)]:
                yield item

        async def list_tasks(self, seed: str, force: bool = False):
            for i in range(n_tasks_per_seed):
                cmd = f"echo 'process''ing {magic_str} {seed} - task {i}X'"
                yield cmd
                expected.add(cmd)

    async with Work() as work:
        async with get_token_credential() as credential:
            async with JobQ.from_service_bus(
                sb_queue, fqns=f"{sb_namespace}.servicebus.windows.net", credential=credential
            ) as jobq:
                await jobq.clear()

                await batch_enqueue(jobq, work)

    with ProcessPoolExecutor(max_workers=n_seeds) as executor:
        executable = shutil.which("ai4s-jobq")
        futures = [
            executor.submit(
                partial(
                    subprocess.run,
                    [executable, f"sb://{sb_namespace}/{sb_queue}", "worker"],
                    stdout=subprocess.PIPE,
                    text=True,
                ),
            )
            for _ in range(n_seeds)
        ]
        results = [f.result() for f in as_completed(futures)]
        output = "\n".join([r.stdout for r in results])
        for seed in range(n_seeds):
            for i in range(n_tasks_per_seed):
                cmd = f"processing {magic_str} seed {seed} - task {i}X"
                assert output.count(cmd) == 1


@pytest.mark.live
@pytest.mark.asyncio
async def test_servicebus_retry(sb_namespace, sb_queue):
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
            if cmd in self.done and self.done[cmd] < 2:
                raise Exception("Simulated failure")
            return f"processed {cmd}"

    async with Work() as work:
        async with get_token_credential() as credential:
            async with JobQ.from_service_bus(
                sb_queue, fqns=f"{sb_namespace}.servicebus.windows.net", credential=credential
            ) as jobq:
                await jobq.clear()

                await batch_enqueue(jobq, work, num_retries=1)
                await launch_workers(jobq, work, num_workers=20, max_consecutive_failures=10000)

    for v in work.done.values():
        assert v == 2

    assert len(work.done) == n_seeds * n_tasks_per_seed
    assert set(work.done.keys()) == expected
