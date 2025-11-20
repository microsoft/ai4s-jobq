from unittest.mock import call

from ai4s.jobq.orchestration.manager import batch_enqueue, launch_workers
from ai4s.jobq.work import Processor, SequentialProcessor, WorkSpecification


async def test_generate(async_queue, mocker):
    async with async_queue.get_worker_interface() as worker_interface:

        class MockWorkerCM:
            async def __aenter__(self):
                return worker_interface

            async def __aexit__(self, exc_type, exc, tb):
                pass

        mocker.patch.object(async_queue, "get_worker_interface", return_value=MockWorkerCM())
        push = mocker.patch.object(async_queue, "push")

        class Gen(WorkSpecification):
            async def list_tasks(self, seed, force):
                yield dict(value=1)
                yield dict(value=2)

        # call simple API for enqueueing
        await batch_enqueue(async_queue, Gen(), num_list_task_workers=1, num_enqueue_workers=1)

        # check that everything was pushed
        assert push.await_count == 2
        push.assert_has_awaits(
            [
                call(
                    dict(value=1),
                    num_retries=1,
                    reply_requested=False,
                    worker_interface=worker_interface,
                ),
                call(
                    dict(value=2),
                    num_retries=1,
                    reply_requested=False,
                    worker_interface=worker_interface,
                ),
            ]
        )


async def test_process(async_queue, mocker):
    await async_queue.push(dict(value=1))
    await async_queue.push(dict(value=2))

    calls = list()

    class Proc(Processor):
        async def __call__(self, **kwargs):
            calls.append(call(**kwargs))

    await launch_workers(async_queue, Proc())
    assert calls == [call(value=1), call(value=2)]


def func(fn: str, value: str) -> None:
    with open(fn, "w") as f:
        f.write(value)


async def test_sequential(async_queue, tmp_path):
    from uuid import uuid4

    ident = str(uuid4())

    fn = tmp_path / "test"

    async with SequentialProcessor(func) as proc:
        await async_queue.push(dict(fn=str(fn), value=ident))
        assert await async_queue.pull_and_execute(proc)
        assert fn.read_text() == ident
