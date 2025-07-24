import asyncio
import functools
import logging
import os
import signal
import typing as ty
import uuid
from contextlib import asynccontextmanager
from datetime import datetime, timedelta
from functools import partial

import aiohttp.client_exceptions
import psutil
import rich.progress
from azure.core.exceptions import ServiceResponseError

try:
    from opentelemetry.trace import get_tracer

    HAVE_OTEL = True
except ImportError:
    HAVE_OTEL = False

    def get_tracer(x) -> None:  # type: ignore
        return None


from contextlib import AsyncExitStack
from functools import wraps
from itertools import chain

from ai4s.jobq import (
    EmptyQueue,
    JobQ,
    JobQFuture,
    Response,
    WorkerCanceled,
    WorkSpecification,
)
from ai4s.jobq.scheduled_events import PreemptionEventHandler
from ai4s.jobq.work import EnqueueStats, Processor

LOG = logging.getLogger(__name__)
TRACE = get_tracer("ai4s.jobq")
T = ty.TypeVar("T")
TaskType = ty.TypeVar("TaskType")
SeedType = ty.TypeVar("SeedType")


try:
    import mlflow  # type: ignore

    HAVE_MLFLOW = True
except ImportError:
    HAVE_MLFLOW = False
    mlflow = None


class TooManyFailuresException(Exception):
    pass


def _mk_task_queue(
    work_spec: WorkSpecification[TaskType, ty.Any],
) -> "asyncio.Queue[TaskType]":
    return asyncio.Queue(maxsize=10000)


def _mk_seed_queue(
    work_spec: WorkSpecification[ty.Any, SeedType],
) -> "asyncio.Queue[SeedType]":
    return asyncio.Queue(maxsize=10000)


def _async_catch_and_print_exc(
    f: ty.Callable[..., ty.Awaitable[T]],
) -> ty.Callable[..., ty.Coroutine[None, None, T]]:
    @wraps(f)
    async def wrapper(*args: ty.Any, **kwargs: ty.Any) -> T:
        try:
            return await f(*args, **kwargs)
        except Exception as e:
            LOG.exception("Caught exception %s", e)
            raise

    return wrapper


async def batch_enqueue(
    queue: JobQ,
    work_spec: ty.Union[WorkSpecification[TaskType, SeedType], ty.Iterable[TaskType]],
    force: bool = False,
    num_retries: int = 1,
    num_list_task_workers: int = 10,
    num_enqueue_workers: int = 100,
    dry_run: bool = False,
    show_progress: bool = True,
    reply_requested: bool = False,
) -> ty.List[JobQFuture]:
    """Lists tasks from a WorkSpecification and pushes them onto a queue.

    This uses asyncio to parallelize the listing and pushing of tasks.

    Args:
        queue: The queue to push tasks onto.
        work_spec: The WorkSpecification to generated tasks with.
        force: If True, tasks will be pushed even if they are considered 'done' by the WorkSpecification.
               The flag gets passed to both list_tasks and enqueue methods.
        num_retries: Number of times a task should be retried by a worker before giving up.
        num_list_task_workers: Degree of parallelism for listing tasks.
        num_enqueue_workers: Degree of parallelism for pushing tasks onto the queue.
        dry_run: If True, tasks will not be pushed onto the queue.
        show_progress: Show a progress bar while working.
    """

    futures = []

    if not isinstance(work_spec, WorkSpecification):
        tasks: ty.Iterable[TaskType] = work_spec

        class DummyWorkSpec(WorkSpecification):
            async def list_tasks(self, SeedType, force: bool = False):
                for task in tasks:
                    yield task

        work_spec = DummyWorkSpec()

    enqueue_jobs = _mk_task_queue(work_spec)
    enum_jobs = _mk_seed_queue(work_spec)
    stats = EnqueueStats("Enqueue")
    columns: ty.List[rich.progress.ProgressColumn] = [
        rich.progress.SpinnerColumn(),
        stats,
    ]

    shutdown_event = asyncio.Event()

    async with AsyncExitStack() as stack:
        await stack.enter_async_context(work_spec)

        if not dry_run:
            await queue.create()

        for cm in work_spec.context_managers:
            if hasattr(cm, "stats"):
                columns.append(getattr(cm, "stats"))
            else:
                columns.append(rich.progress.TextColumn(work_spec.__class__.__qualname__))

        if show_progress:
            progress = stack.enter_context(rich.progress.Progress(*columns, refresh_per_second=1))
            task_id = progress.add_task("Enqueued", total=None)

        @_async_catch_and_print_exc
        async def enum_worker() -> None:
            assert isinstance(work_spec, WorkSpecification)
            while True:
                work = await enum_jobs.get()
                try:
                    if work is None:
                        break
                    async for tpl in work_spec.list_tasks(work, force=force):
                        await enqueue_jobs.put(tpl)
                finally:
                    enum_jobs.task_done()

        @_async_catch_and_print_exc
        async def enqueue_worker() -> None:
            assert isinstance(work_spec, WorkSpecification)
            while True:
                work = await enqueue_jobs.get()
                if work is None:
                    break
                try:
                    job = await work_spec.enqueue_task(work, force=force)
                    stats.n_considered += 1
                    if job is not None:
                        stats.n_queued += 1
                        if not dry_run:
                            fut = await queue.push(
                                job,
                                num_retries=num_retries,
                                reply_requested=reply_requested,
                            )
                            futures.append(fut)
                    if show_progress:
                        progress.update(task_id, advance=1)
                    enqueue_jobs.task_done()
                except Exception as e:
                    LOG.exception("Caught exception during enqueue: %s", e)
                    enqueue_jobs.task_done()  # ensure worker can exit
                    break

        enum_workers: ty.List[asyncio.Task[None]] = [
            asyncio.create_task(enum_worker(), name=f"enum-worker-{idx}")
            for idx in range(num_list_task_workers)
        ]
        enq_workers: ty.List[asyncio.Task[None]] = [
            asyncio.create_task(enqueue_worker(), name=f"enq-worker-{idx}")
            for idx in range(num_enqueue_workers)
        ]

        async for tpl in work_spec.task_seeds():
            await enum_jobs.put(tpl)

        async def clean_shutdown() -> None:
            LOG.info("Waiting for queue to complete")
            await enum_jobs.join()
            await enqueue_jobs.join()

            LOG.info("Joining workers")
            for worker in chain(enum_workers, enq_workers):
                worker.cancel()

            # Give all workers a chance to finish
            for fut in asyncio.as_completed(list(chain(enum_workers, enq_workers)), timeout=30):
                try:
                    await fut
                except asyncio.CancelledError:
                    pass

        clean_shutdown_task = asyncio.create_task(clean_shutdown(), name="clean-shutdown")
        shutdown_event_task = asyncio.create_task(
            shutdown_event.wait(), name="shutdown-event-waiter"
        )

        def shutdown_handler() -> None:
            """Handles shutdown signals."""
            shutdown_event.set()

        loop = asyncio.get_running_loop()
        loop.add_signal_handler(signal.SIGTERM, shutdown_handler)
        loop.add_signal_handler(signal.SIGINT, shutdown_handler)

        done, pending = await asyncio.wait(
            [clean_shutdown_task, shutdown_event_task],
            return_when=asyncio.FIRST_COMPLETED,
        )
        if shutdown_event_task in done:
            LOG.info("Shutdown event set. Cancelling workers.")
            pending.update(enum_workers)
            pending.update(enq_workers)
            for task in pending:
                task.cancel()
            await asyncio.gather(*pending, return_exceptions=True)
            LOG.info("Workers cancelled.")

    return futures


async def get_results(
    futures: ty.Iterable[JobQFuture],
    n_workers: int = 32,
) -> ty.AsyncGenerator[Response, None]:
    """Retrieves the results of tasks from the queue.

    Args:
        queue: The queue to pull tasks from.
        session_ids: The session ids of the tasks to retrieve.
        timeout: The maximum time to wait for the results.

    Returns:
        The results of the tasks.
    """

    sem = asyncio.Semaphore(n_workers)

    async def safe_wait(fut: JobQFuture) -> Response:
        async with sem:
            return await fut

    tasks = [asyncio.create_task(safe_wait(fut)) for fut in futures]
    for result in asyncio.as_completed(tasks):
        yield await result


async def launch_workers(
    queue: JobQ,
    processor: Processor,
    time_limit: timedelta = timedelta(days=1),
    visibility_timeout: timedelta = timedelta(hours=1),
    with_heartbeat: bool = True,
    max_consecutive_failures: int = 10,
    num_workers: int = 1,
    show_progress: bool = True,
    worker_id: ty.Optional[str] = None,
    environment_name: str = "",
) -> None:
    """Launches multiple workers to pull and execute tasks from a queue.

    Args:
        queue: The queue to pull tasks from.
        processor: Will be called with the task payload.
        time_limit: Soft time limit in seconds. No new tasks will be started after this time limit is reached.
        visibility_timeout: Number of seconds to reserve a task while working on it, hiding it from other workers.
        with_heartbeat: If True, the visibility timeout will be extended indefinitely while the task is running.
        max_consecutive_failures: Maximum number of consecutive failures before exiting.
        num_workers: Number of workers to launch.
        show_progress: Show a progress bar while working.
        worker_id: A unique identifier for the worker. If None, a random id will be generated.
        environment_name: A string descriptor of the runtime environment, for example the cluster name, used for logging.
    """
    if worker_id is None:
        worker_id = uuid.uuid4().hex

    async with AsyncExitStack() as stack:
        # Periodic log message that is used in the Grafana dashboard to report # active users
        # todo: consider using utilities.async_utils.call_periodically instead
        await stack.enter_async_context(
            _call_periodically(
                lambda: worker_heartbeat_fn(
                    worker_id=str(worker_id),
                    queue=queue,
                    environment_name=environment_name,
                ),
                interval=timedelta(minutes=5),
            )
        )

        if hasattr(processor, "stats") and hasattr(processor.stats, "dict"):
            if HAVE_MLFLOW:
                await stack.enter_async_context(
                    _call_periodically(
                        functools.partial(_log_stats_to_mlflow, processor.stats),
                        interval=timedelta(minutes=1),
                    )
                )
            else:
                LOG.warning("MLFlow is not installed. Not logging stats to ML.")

        columns: ty.List[rich.progress.ProgressColumn] = [rich.progress.SpinnerColumn()]
        for cm in processor.context_managers:
            if hasattr(cm, "stats"):
                columns.append(getattr(cm, "stats"))
            else:
                columns.append(rich.progress.TextColumn(processor.__class__.__qualname__))
        if hasattr(processor, "stats"):
            columns.append(getattr(processor, "stats"))

        if show_progress:
            progress = stack.enter_context(rich.progress.Progress(*columns, refresh_per_second=1))
            task_id = progress.add_task("Enqueued", total=None)

        # This event will be set whenever we get preempted.
        shutdown_event = asyncio.Event()
        hard_stop_event = asyncio.Event()

        # there are two mechanisms for preemption: either we get a SIGTERM signal (singularity, or workforce)
        # or the AML service announces that the current node will get preempted.

        # The PreemptionEventHandler takes care of the AML service
        await stack.enter_async_context(
            PreemptionEventHandler(
                shutdown_event=shutdown_event,
                worker_id=str(worker_id),
                queue=queue,
                environment_name=environment_name,
                poll_interval_seconds=1,
            )
        )

        # This signal handler takes care of the SIGTERM signal.
        def shutdown_handler(*args: ty.Any, hard=False) -> None:
            LOG.info("Shutdown requested. Setting shutdown event.")
            if hard:
                LOG.info(
                    "Hard shutdown requested, will not wait for a potentially impeding preemption."
                )
                hard_stop_event.set()
            else:
                LOG.info(
                    "Soft shutdown requested. Will not accept additional tasks, cancel current one(s), and sleep until this process is terminated by an imminent preemption."
                )
            if shutdown_event.is_set():
                if not hard_stop_event.is_set():
                    LOG.info("Shutdown event already set. Trying to shut down immediately.")
                    hard_stop_event.set()
                else:
                    LOG.info("Hard shutdown event already set. Stopping event loop.")
                    asyncio.get_event_loop().stop()
            shutdown_event.set()

        async def _wait_for_clear(event):
            while event.is_set():
                await asyncio.sleep(1)

        async def wait_for_hard_stop(processor) -> bool:
            """Waits for the hard stop event to be set."""
            if not hard_stop_event.is_set():
                LOG.info("Bracing for an imminent preemption.")
                tasks = [
                    asyncio.create_task(hard_stop_event.wait(), name="hard-stop-waiter"),
                    asyncio.create_task(
                        _wait_for_clear(shutdown_event), name="preemption-clear-waiter"
                    ),
                ]
                preemption_timeout = int(os.getenv("JOBQ_PREEMPTION_TIMEOUT", "120"))
                try:
                    await asyncio.wait_for(
                        asyncio.wait(tasks, return_when=asyncio.FIRST_COMPLETED),
                        timeout=preemption_timeout,
                    )
                except asyncio.TimeoutError:
                    LOG.info(
                        "No preemption occurred within %d seconds. Resuming work.",
                        preemption_timeout,
                    )

                for task in tasks:
                    task.cancel()
                await asyncio.gather(*tasks, return_exceptions=True)

                if hard_stop_event.is_set():
                    LOG.info("Hard stop event set. Will not resume work.")
                elif not shutdown_event.is_set():
                    # Azure decided that the announced preemption is not going to happen
                    LOG.info("Shutdown event not set (anymore). Resuming work.")

                if not hard_stop_event.is_set():
                    shutdown_event.clear()
                    await processor.resume()
                    return False
            return True

        # preemption
        signal.signal(signal.SIGTERM, partial(shutdown_handler, hard=False))
        # workforce / user-initiated shutdown
        signal.signal(signal.SIGINT, partial(shutdown_handler, hard=True))

        async def worker(idx: int) -> None:
            num_consecutive_failures = 0
            soft_limit_time = datetime.now() + time_limit
            async with AsyncExitStack() as worker_stack:
                if TRACE:
                    span = worker_stack.enter_context(
                        TRACE.start_as_current_span("ai4s.jobq.worker")
                    )
                    if worker_id:
                        span.set_attribute("worker_id", worker_id)
                    span.set_attribute("asyncio_idx", idx)
                    span.set_attribute("queue", queue.full_name)
                    span.set_attribute("environment", os.environ.get("JOBQ_ENVIRONMENT_NAME", ""))

                if run_id := os.getenv("AZUREML_RUN_ID"):
                    workspace_scope = os.getenv("AZUREML_WORKSPACE_SCOPE")
                    LOG.info(
                        f"Worker {worker_id} ({idx}) for queue {queue.full_name} started in https://ml.azure.com/runs/{run_id}?wsid={workspace_scope}",
                        {
                            "aml_run_id": run_id,
                            "aml_experiment_id": os.getenv("AZUREML_EXPERIMENT_ID"),
                            "aml_workspace_name": os.getenv("AZUREML_ARM_WORKSPACE_NAME"),
                            "queue": queue.full_name,
                        },
                    )

                while True:
                    if datetime.now() > soft_limit_time:
                        LOG.error(
                            "The soft time limit was exceeded. Exiting.",
                        )
                        return
                    shutdown_event_task = asyncio.create_task(
                        shutdown_event.wait(), name=f"shutdown-event-{idx}"
                    )
                    try:
                        worker_coro = queue.pull_and_execute(
                            processor,
                            visibility_timeout=visibility_timeout,
                            with_heartbeat=with_heartbeat,
                            worker_id=worker_id,
                        )
                        # convert coro to a task
                        worker_task = asyncio.create_task(worker_coro, name=f"worker-{idx}")
                        done, pending = await asyncio.wait(
                            [worker_task, shutdown_event_task],
                            return_when=asyncio.FIRST_COMPLETED,
                        )
                        if shutdown_event_task in done:
                            assert (
                                shutdown_event.is_set()
                            ), "Shutdown event should be set if we got here"
                            # Cancel the worker if shutdown was requested (most likely preemption)
                            LOG.info("Worker %d shutdown requested.", idx)
                            for task in pending:
                                task.cancel()
                                try:
                                    await task
                                except asyncio.CancelledError:
                                    pass
                            LOG.info("Worker %d canceled.", idx)
                            if not await wait_for_hard_stop(processor):
                                continue
                            return
                        else:
                            assert worker_task in done, "Worker task should be done if we got here"
                            success = await worker_task

                        if show_progress:
                            progress.update(task_id, advance=1)
                        if success:
                            num_consecutive_failures = 0
                        else:
                            num_consecutive_failures += 1
                            if num_consecutive_failures > max_consecutive_failures:
                                LOG.error(
                                    "Maximum number of consecutive failures reached. Exiting."
                                )
                                raise TooManyFailuresException()
                    except WorkerCanceled:
                        LOG.info("Worker %d canceled.", idx)
                        if not await wait_for_hard_stop(processor):
                            continue
                        return
                    except EmptyQueue:
                        LOG.info("Worker %d finished: Queue is empty.", idx)
                        return
                    finally:
                        try:
                            shutdown_event_task.cancel()
                            await shutdown_event_task
                        except asyncio.CancelledError:
                            pass

        worker_tasks = [asyncio.create_task(worker(idx)) for idx in range(num_workers)]
        for coro in asyncio.as_completed(worker_tasks):
            try:
                await coro  # type: ignore
            except asyncio.CancelledError:
                pass
            except TooManyFailuresException:
                LOG.error("Too many consecutive failures. Canceling all workers.")
                for task in worker_tasks:
                    task.cancel()
                for fut in asyncio.as_completed(worker_tasks, timeout=30):
                    try:
                        await fut
                    except asyncio.CancelledError:
                        pass
                raise


@asynccontextmanager
async def _call_periodically(
    callback_fn: ty.Callable[[], ty.Awaitable[None]],
    *,
    interval: timedelta = timedelta(seconds=5),
) -> ty.AsyncGenerator[None, None]:
    """Logs a message periodically"""

    async def _heartbeat() -> None:
        while True:
            await callback_fn()
            await asyncio.sleep(interval.total_seconds())

    task = asyncio.create_task(_heartbeat(), name="heartbeat")

    yield

    task.cancel()

    try:
        await task
    except asyncio.CancelledError:
        pass


# We periodically log a 'heartbeat' message to indicate that the worker is still running.
# This is used for dashboarding.
@_async_catch_and_print_exc
async def worker_heartbeat_fn(worker_id: str, queue: JobQ, environment_name: str) -> None:
    try:
        queue_size = await queue.get_approximate_size()
    except (ServiceResponseError, aiohttp.client_exceptions.ClientConnectorError):
        queue_size = -1
    LOG.info(
        "Worker is still running. Approximate queue size=%d.",
        queue_size,
        extra={
            "worker_id": worker_id,
            "queue": queue.full_name,
            "environment": environment_name,
            "cpu_util": psutil.cpu_percent() / 100,
            "memory_util": psutil.virtual_memory().percent / 100,
            "queue_size": queue_size,
        },
    )


class _StatsObjectWithDict(ty.Protocol):
    def dict(self) -> ty.Dict[str, ty.Any]: ...


@_async_catch_and_print_exc
async def _log_stats_to_mlflow(stats: _StatsObjectWithDict) -> None:
    """This is meant to run periodically and log processor stats to MLFlow."""
    assert HAVE_MLFLOW

    if "JOBQ_MLFLOW_LOG_PREFIX" in os.environ:
        prefix = os.environ["JOBQ_MLFLOW_LOG_PREFIX"] + "_"
    elif "RANK" in os.environ:
        prefix = f"rank_{os.environ['RANK']}_"
    else:
        prefix = "_"

    mlflow.log_metrics({prefix + key: value for key, value in stats.dict().items()})
