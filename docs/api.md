# Python API

The Python API is mainly interesting if you want to run small tasks where the
shell has too much overhead. Examples are:

- You want to interleave data loading/storing and processing
- You're I/O constrained and want to run *many* tasks in parallel

You get the most out of the Python API if you write (...and your performance
depends on...) `async` code, but it's not strictly necessary.

## Queueing and Running

Rather than submitting tasks one by one using the CLI as shown in the basic example, you can implement the `ai4s.jobq.orchestration.WorkSpecification` protocol in Python to list all your tasks quickly. 

```python
from azure.identity import AzureCliCredential
from ai4s.jobq import JobQ, WorkSpecification


class NumberSquaring(WorkSpecification):
  async def list_tasks(self, seed=None, force=False):
    # Here you define the tasks that you want to run.
    # Tasks are dictionaries that will be passed to the __call__ method below.
    # See the WorkSpecification protocol for more options.
    for i in range(10):
      yield dict(my_number=i)  # kwargs of the `square()` method below


work_specification = NumberSquaring()
```

You then enqueue tasks like this:

```python
from ai4s.jobq import batch_enqueue
from ai4s.auth import get_token_credential


async with get_token_credential() as cred:
  async with JobQ.from_storage_queue("test-queue", storage_account="mystorageaccount", credential=cred) as jobq:
    await batch_enqueue(jobq, work_specification)
    # or, equivalently:
    await batch_enqueue(jobq, [dict(my_number=i) for i in range(10)])  # kwargs of the `square()` method below
```

And running multiple workers (in parallel with asyncio) looks like this:

```python
from ai4s.jobq import launch_workers, SequentialProcessor


async def square(my_number):
    # Here you define the work that you want to do for each task.
    print(f"{my_number} squared is {my_number**2}.")


async with get_token_credential() as cred:
  async with JobQ.from_storage_queue("test-queue", storage_account="ai4science0eastus", credential=cred) as jobq:
    await launch_workers(
      jobq,
      square,
      num_workers=10
    )
```



```python
from ai4s.auth import get_token_credential


async with get_token_credential() as cred:
  async with JobQ.from_service_bus("test-queue", fqns="mysb.servicebus.windows.net", credential=cred) as jobq:
    ...
```

Note that `square` is `async`, but does not do any asynchronous operations and
never yields control. This blocks jobq from telling the backend that the task
is still being processed ("heartbeat"). That's  OK if it is running for less
than the configured visibility timeout.

If your task runs longer than the configured visibility timeout, the backend
may consider the worker as crashed and give the same task to another worker. In
these cases, use the `SequentialProcessor` wrapper, which offloads the
computation to a separate process. This allows jobq to send a heartbeat to the
backend in regular intervals.

```python
from ai4s.jobq import SequentialProcessor


# NOTE: *not* async here!
def square(my_number):
    # Here you define the work that you want to do for each task.
    print(f"{my_number} squared is {my_number**2}.")


async with JobQ.from_storage_queue("test-queue", storage_account="ai4science0eastus", credential=AzureCliCredential()) as jobq:
  async with SequentialProcessor(square) as processor:
    await launch_workers(jobq, processor)
```

## Multiple Workers

If your tasks are genuinely asynchronous, ie, they mostly call asynchronous APIs, you can just set `num_workers=5` etc. when calling launch_workers.

Otherwise, you can use a `ProcessPool` to make sure computationally-intensive work can be parallelized and does not block the queue.

```python
import os
import time
from functools import partial
from ai4s.jobq import WorkSpecification, ProcessPool, Processor


class NumberSquaring(WorkSpecification):
  async def list_tasks(self, seed=None, force=False):
    for i in range(10):
      await self.pool.submit(partial(time.sleep, 5))  # Compute intensive task
      yield dict(my_number=i)   # kwargs of the processor's __call__ below


class NumberSquaringProcessor(Processor):
  def __init__(self):
    super().__init__()
    self.pool = ProcessPool(pool_size=os.cpu_count())
    self.register_context_manager(self.pool)

  async def __call__(self, my_number):
    await self.pool.submit(partial(time.sleep, 5))  # Compute intensive task
    print(f"{my_number} squared is {my_number**2}.")


async with JobQ.from_storage_queue("test-queue", storage_account="ai4science0eastus", credential=cred) as jobq:
  async with NumberSquaringProcessor() as processor:
    await launch_workers(jobq, processor)
```

## Multi-Worker Logging

You can contextualize your logs by writing worker and job ID. To achieve this,
add the magic `_job_id` and `_worker_id` string parameters to your callback:

```python
  async def __call__(self, my_number: int, _job_id: str, _worker_id: str):
    logger = logging.getLogger(f"task.{_worker_id}.{_job_id}")
    logger.info("Working on %d", my_number)
    ...
```


## Writing an entry point

A common use case is to simply call a function for every item in the queue:

```python
import asyncio
from ai4s.jobq import SequentialProcessor, launch_workers, JobQ, setup_logging


def my_cpu_intensive_work(**kwargs):
  ...


async def main():
  async with JobQ.from_environment() as jobq:
    setup_logging(jobq.full_name)
    async with SequentialProcessor(my_cpu_intensive_work) as proc:
      await launch_workers(jobq, proc)

asyncio.run(main())
```

The `kwargs` correspond to the `dict` you queued. `my_cpu_intensive_work` is
automatically run in a process pool of size 1. The `from_environment`
constructor is a shortcut that relies on the environment variables set by
`ai4s-jobq QUEUE_SPEC amlt`: `JOBQ_STORAGE` and `JOBQ_QUEUE`.

## Working with blob storage efficiently

```python
from ai4s.jobq import WorkSpecification
from ai4s.jobq.blob import BlobContainer
from tempfile import TemporaryDirectory
import os


class BlobSizeCounting(WorkSpecification):
  def __init__(self):
    super().__init__()
    self.container = BlobContainer(storage_account="mystorageaccount", container="my-data")
    self.register_context_manager(self.container)

  async def task_seeds(self):
    # You can use top-level directories in the blob storage container to parallelize the listing of blobs.
    # `list_tasks` will be called in parallel for each of these 'seeds'.
    walk = self.container.client.walk_blobs(name_starts_with="")
    async for directory in walk:
      yield directory.name

  async def list_tasks(self, seed, force=False):
    async for blob in self.container.client.list_blobs(name_starts_with=seed):
      yield {"blob": blob.name}

  # Note: we include __call__ here in the WorkSpecification because it's
  # logically related to how tasks are listed. You can keep the Processor entirely
  # separate though, if you prefer.
  async def __call__(self, blob, **kwargs):
    # Download the blob and report its size (just as an example).
    with TemporaryDirectory(dir="/dev/shm") as tmpdir:
      filename = await self.container.download_file(blob, tmpdir)
      print(f"{blob} is {os.path.getsize(filename)} bytes.")

work_specification = BlobSizeCounting()
```

Similar to `download_file`, there's also `upload_file` and `upload_from_folder`.
The latter uploads all files in the folder concurrently.

## Checking if work has already been done before enqueing

You can overload the `already_done` method to prevent a listed item from queueing.
This is useful when checking the item takes time. There's a separate worker
pool that does this checking, increasing the overall efficiency. The size of
the worker pools for `list_task` and `already_done` can be parameterized with the
`batch_enqueue` function.

```python
class ZipAll(WorkSpecification):
  def __init__(self):
    super().__init__()
    self.container = BlobContainer(storage_account="mystorageaccount", container="my-data")
    self.register_context_manager(self.container)

  async def list_tasks(self, seed, force=False):
    async for blob in self.container.client.list_blobs(name_starts_with=""):
      if not blob.name.endswith(".zip")
        yield {"blob": blob.name}

  async def already_done(self, blob: str):
    # return value True will cause the item not to be queued.
    return await self.container.blob_exists(blob + ".zip")

work_specification = ZipAll()
```
