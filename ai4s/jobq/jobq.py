# Copyright (c) Microsoft Corporation.
# Licensed under the MIT License.
import asyncio
import logging
import os
import textwrap
import time
from contextlib import asynccontextmanager, suppress
from dataclasses import replace
from datetime import timedelta
from functools import partial
from importlib.metadata import PackageNotFoundError
from importlib.metadata import version as pkg_version
from inspect import signature
from typing import (
    Any,
    AsyncGenerator,
    Awaitable,
    Callable,
    Generator,
    TypeVar,
)

import azure.core.exceptions
from azure.core.credentials_async import AsyncTokenCredential
from packaging.requirements import Requirement
from packaging.version import Version

from ai4s.jobq.entities import LockLostError, Response, Task, WorkerCanceled

from .backend.common import JobQBackend, JobQBackendWorker

LOG = logging.getLogger("ai4s.jobq")
T = TypeVar("T", bound="JobQ")
CallbackReturnType = TypeVar("CallbackReturnType")


def _is_async_callable(obj: Any) -> bool:
    while isinstance(obj, partial):
        obj = obj.func

    return asyncio.iscoroutinefunction(obj) or (
        callable(obj) and asyncio.iscoroutinefunction(obj.__call__)
    )


class JobQFuture(Awaitable[Response]):
    def __init__(self, jobq: "JobQ", job_id: str):
        self.jobq = jobq
        self.job_id = job_id
        self._result: Response | None = None

    def __await__(self) -> Generator[Any, None, Response]:
        async def closure() -> Response:
            if self._result is not None:
                raise RuntimeError("This future has already been awaited.")
            self._result = await self.jobq.get_result(self.job_id)
            return self._result

        return closure().__await__()

    def result(self, timeout: timedelta | None = None) -> Response:
        async def closure() -> Response:
            if self._result is not None:
                return self._result
            self._result = await self.jobq.get_result(self.job_id, timeout)
            return self._result

        return asyncio.run(closure())


class JobQ:
    def __init__(
        self,
        backend: JobQBackend,
        credential: str | AsyncTokenCredential | None = None,
    ):
        self._client = backend
        self._credential = credential

    @classmethod
    @asynccontextmanager
    async def from_environment(
        cls: type[T],
        *,
        exist_ok: bool = True,
    ) -> AsyncGenerator[T, None]:
        """Creates a new queue from environment variables set by `ai4s-jobq amlt`."""

        jobq_storage = os.environ.get("JOBQ_STORAGE")
        jobq_queue = os.environ.get("JOBQ_QUEUE")
        if not jobq_storage:
            raise ValueError("JOBQ_STORAGE environment variable not set.")
        if not jobq_queue:
            raise ValueError("JOBQ_QUEUE environment variable not set.")
        if jobq_storage.startswith("sb://"):
            fqns = jobq_storage[len("sb://") :] + ".servicebus.windows.net"
            async with cls.from_service_bus(
                jobq_queue,
                fqns=fqns,
                credential=None,
                exist_ok=exist_ok,
            ) as jobq:
                yield jobq
        else:
            async with cls.from_storage_queue(
                jobq_queue,
                storage_account=jobq_storage,
                credential=None,
                exist_ok=exist_ok,
            ) as jobq:
                yield jobq

    @classmethod
    @asynccontextmanager
    async def from_service_bus(
        cls: type[T],
        name: str,
        *,
        fqns: str | None = None,
        credential: Any | None = None,
        exist_ok: bool = True,
        duplicate_detection_window: timedelta | None = None,
    ) -> AsyncGenerator[T, None]:
        """Creates a new queue from a Service Bus."""
        from .backend.servicebus_rest import ServiceBusRestBackend

        assert fqns is not None
        async with ServiceBusRestBackend(
            queue_name=name,
            fqns=fqns,
            credential=credential,
            exist_ok=exist_ok,
            duplicate_detection_window=duplicate_detection_window,
        ) as backend:
            str_credential = credential if isinstance(credential, str) else None
            yield cls(backend, credential=str_credential)

    @classmethod
    @asynccontextmanager
    async def from_storage_queue(
        cls: type[T],
        name: str,
        *,
        storage_account: str,
        credential: str | AsyncTokenCredential | None,
        exist_ok: bool = True,
        **kwargs: Any,
    ) -> AsyncGenerator[T, None]:
        """Creates a new queue from a storage account."""
        from .backend.storage_queue import StorageQueueBackend

        async with StorageQueueBackend(
            queue_name=name,
            storage_account=storage_account,
            credential=credential,
        ) as backend:
            await backend.create(exist_ok=exist_ok)
            yield cls(backend, credential=credential)

    async def get_approximate_size(self) -> int:
        return await self._client.__len__()

    async def sas_token(self, ttl: timedelta | None) -> str:
        """Generates a Shared Access Token (SAS) to grant access to a worker.

        Args:
            ttl (timedelta): The token expires after this amount of time. Default: 14 days.
        """
        if ttl is None:
            ttl = timedelta(days=14)

        return self._client.generate_sas(ttl)

    @classmethod
    @asynccontextmanager
    async def from_connection_string(
        cls: type[T],
        name: str,
        *,
        connection_string: str,
        exist_ok: bool = True,
        **kwargs: Any,
    ) -> AsyncGenerator[T, None]:
        """Creates a new queue from a connection string."""

        fields = connection_string.split(";")
        field_dct = {field.split("=", 1)[0]: field.split("=", 1)[1] for field in fields if field}
        credential = (
            field_dct.get("SharedAccessSignature")
            or field_dct.get("SharedAccessKey")
            or field_dct.get("AccountKey")
        )

        backend: JobQBackend
        if "QueueEndpoint" in connection_string:
            from .backend.storage_queue import StorageQueueBackend

            backend = StorageQueueBackend(
                queue_name=name,
                connection_string=connection_string,
                credential=credential,
            )
        else:
            raise ValueError(f"Unknown connection string type: {connection_string}")

        async with backend:
            self = cls(backend, credential=None)
            await self.create(exist_ok=exist_ok)
            yield self

    async def create(self, *, exist_ok: bool = True) -> None:
        """creates the queue if necessary."""
        try:
            await self._client.create(exist_ok=exist_ok)
        except azure.core.exceptions.ResourceExistsError:
            if exist_ok:
                pass
            else:
                raise

    async def peek(self, n: int = 1, as_json: bool = False) -> Any:
        """Peeks at the next task in the queue."""
        return await self._client.peek(n, as_json)

    async def clear(self) -> None:
        """Empties the queue."""
        await self._client.clear()

    async def get_result(self, session: str, timeout: timedelta | None = None) -> Response:
        """Retrieves the result of a task from the queue."""
        return await self._client.get_result(session, timeout)

    async def push(
        self,
        kwargs: dict[str, Any] | str,
        *,
        num_retries: int = 5,
        reply_requested: bool = False,
        min_version: str | None = None,
        id: str | None = None,
        worker_interface: JobQBackendWorker | None = None,
    ) -> JobQFuture:
        """Pushes a command to a queue.

        Args:
            kwargs: The bash command to push, or a dict passed to the processor.
            num_retries: The number of times to retry the command if it returns a non-zero exit code.

        Returns:
            Auto-generated unique ID for the task.
        """
        # convenience shortcut for ShellCommandProcessor
        if isinstance(kwargs, str):
            kwargs = {"cmd": kwargs}

        task_kwargs: dict[str, Any] = {
            "kwargs": kwargs,
            "num_retries": num_retries,
            "reply_requested": reply_requested,
        }
        if min_version is not None:
            task_kwargs["min_version"] = min_version
        if id is not None:
            task_kwargs["id"] = id
        task = Task(**task_kwargs)
        worker_interface = worker_interface if worker_interface is not None else self._client
        return JobQFuture(self, await worker_interface.push(task))

    @property
    def full_name(self) -> str:
        return self._client.name

    @asynccontextmanager
    async def get_worker_interface(self, **kwargs) -> AsyncGenerator[JobQBackendWorker, None]:
        async with self._client.get_worker_interface(**kwargs) as worker_interface:
            yield worker_interface

    async def pull_and_execute(
        self,
        command_callback: Callable[..., Awaitable[CallbackReturnType]]
        | Callable[..., CallbackReturnType],
        *,
        visibility_timeout: timedelta = timedelta(minutes=10),
        with_heartbeat: bool = False,
        worker_id: str | None = None,
        worker_interface: JobQBackendWorker | None = None,
    ) -> bool | None:
        """Gets one task from the queue (first pushed, first pulled), verifies the signature, and executes the command.

        If the signature is invalid, the task is deleted from the queue.
        If the command fails, the task is re-queued with one less retry.
        If the command succeeds, the task is deleted from the queue.

        Args:
            visibility_timeout: If we send no heartbeat for this many seconds, the task is re-queued.

        Raises:
            EmptyQueue: If the queue is empty.

        Returns:
            True if the task was executed successfully, False if it failed,
            None if the task was requeued due to a version mismatch.
        """
        # Export this environment variable such that inside the jobs we can know which queue we're working on.
        os.environ["JOBQ_QUEUE_NAME"] = self.full_name
        worker_interface = worker_interface if worker_interface is not None else self._client
        async with worker_interface.receive_message(
            visibility_timeout=visibility_timeout,
            with_heartbeat=with_heartbeat,
        ) as envelope:
            task = envelope.task

            # Version gate: requeue if the installed package is too old.
            if task.min_version is not None:
                req = Requirement(task.min_version)
                try:
                    installed = Version(pkg_version(req.name))
                except PackageNotFoundError:
                    LOG.warning(
                        "Task %s requires %s, but package %r is not installed. Requeuing.",
                        task.id,
                        task.min_version,
                        req.name,
                    )
                    await envelope.requeue()
                    await asyncio.sleep(5)
                    return None

                if installed not in req.specifier:
                    LOG.warning(
                        "Task %s requires %s, but installed version is %s. Requeuing.",
                        task.id,
                        task.min_version,
                        installed,
                    )
                    await envelope.requeue()
                    await asyncio.sleep(5)
                    return None

            kwargs = task.kwargs

            # Log the starting of the task
            summary = ", ".join(f"{k}={v}" for k, v in kwargs.items())
            indented_command = textwrap.indent(summary, prefix=">>> ")
            LOG.info(
                f"Task starting {task.id}.\n{indented_command}",
                extra={
                    "task_id": task.id,
                    "event": "task_start",
                },
            )
            start_time = time.time()
            exc = None
            ret: CallbackReturnType | None = None
            lock_lost = False
            try:
                if signature(command_callback).parameters.get("_job_id") is not None:
                    kwargs["_job_id"] = task.id
                if signature(command_callback).parameters.get("_worker_id") is not None:
                    kwargs["_worker_id"] = worker_id
                if _is_async_callable(command_callback):
                    callback_task = asyncio.ensure_future(command_callback(**kwargs))  # type: ignore
                    lock_lost_task = asyncio.ensure_future(envelope.lock_lost_event.wait())
                    try:
                        done, _pending = await asyncio.wait(
                            [callback_task, lock_lost_task],
                            return_when=asyncio.FIRST_COMPLETED,
                        )
                    except asyncio.CancelledError:
                        # Outer task was cancelled (e.g. worker shutdown) —
                        # propagate cancellation to the sub-tasks, then await
                        # the callback so it can clean up (e.g. SIGTERM
                        # subprocesses).  Whatever the callback raises
                        # (WorkerCanceled, CancelledError, etc.) propagates.
                        LOG.info(
                            "Task %s: outer task cancelled (worker shutdown / preemption), "
                            "cancelling callback.",
                            task.id,
                        )
                        lock_lost_task.cancel()
                        with suppress(asyncio.CancelledError):
                            await lock_lost_task
                        callback_task.cancel()
                        await callback_task
                    if lock_lost_task in done:
                        # Lock was lost — cancel the callback and walk away.
                        LOG.info(
                            "Task %s: lock lost, cancelling callback.",
                            task.id,
                        )
                        callback_task.cancel()
                        with suppress(asyncio.CancelledError, WorkerCanceled, Exception):
                            await callback_task
                        lock_lost = True
                    else:
                        lock_lost_task.cancel()
                        with suppress(asyncio.CancelledError):
                            await lock_lost_task
                        ret = callback_task.result()
                        # avoid race condition where the two tasks finished almost at the same time
                        lock_lost = envelope.lock_lost_event.is_set()
                else:
                    LOG.warning(
                        "Callback does not seem to be async. This is problematic if you're using heartbeats and/or multiple workers."
                    )
                    ret = command_callback(**kwargs)  # type: ignore
                    lock_lost = envelope.lock_lost_event.is_set()
                # this value will be ignored when the lock was lost:
                execution_was_succesful = True
            except WorkerCanceled:
                await envelope.cancel_heartbeat()
                try:
                    await envelope.requeue()
                except Exception:
                    LOG.warning(
                        "Failed to requeue task %s (message will be re-delivered "
                        "after the lock expires).",
                        task.id,
                        exc_info=True,
                    )
                duration = time.time() - start_time
                LOG.info(
                    f"Task {task.id} canceled.",
                    extra={
                        "duration_s": duration,
                        "task_id": task.id,
                        "event": "task_canceled",
                    },
                )
                raise
            except Exception as e:
                exc = e  # this roundabout way makes mypy happy.
                execution_was_succesful = False
                lock_lost = envelope.lock_lost_event.is_set()
                await envelope.cancel_heartbeat()
            else:
                await envelope.cancel_heartbeat()

            duration = time.time() - start_time

            if lock_lost:
                LOG.warning(
                    f"Lock lost for task {task.id} — abandoning without settlement "
                    f"(another worker will handle it).",
                    extra={
                        "duration_s": duration,
                        "task_id": task.id,
                        "event": "task_lock_lost",
                    },
                )
                raise LockLostError(f"Lock lost for task {task.id} after {duration:.1f}s")

            if execution_was_succesful:
                LOG.info(
                    f"Completed task {task.id} successfully.",
                    extra={
                        "duration_s": duration,
                        "task_id": task.id,
                        "event": "task_success",
                    },
                )
                try:
                    LOG.debug("Deleting message")
                    if task.reply_requested:
                        await envelope.reply(Response(is_success=True, body=ret))
                    await envelope.delete(success=True)
                except Exception as e:
                    LOG.warning("Failed to delete message %s: %s", envelope.id, e)
                return True
            # get our caching log handler
            log_handler = next(
                (h for h in logging.getLogger().handlers if hasattr(h, "get_log_cache")), None
            )
            log = None
            if log_handler:
                log = "\n".join(log_handler.get_log_cache(task.id))
                log = log[-100 * 256 :]  # truncate to equivalent of 100 lines of 256 chars

            LOG.exception(
                f"Failure for task {task.id}.",
                exc_info=exc,
                extra={
                    "duration_s": duration,
                    "task_id": task.id,
                    "event": "task_failure",
                    "log": log,
                },
            )

            if task.num_retries <= 0:
                LOG.error(
                    f"Out of retries for task {task.id}. Giving up.",
                    extra={
                        "task_id": task.id,
                        "event": "task_give_up",
                    },
                )
                if task.reply_requested:
                    await envelope.reply(Response(is_success=False, body=str(exc)))
                await envelope.delete(success=False, error=str(exc))
                return False

            LOG.info(
                f"Re-queued task {task.id} with {task.num_retries} retries left.",
                extra={
                    "task_id": task.id,
                    "event": "task_retry",
                    "num_retries": task.num_retries - 1,
                },
            )
            task = replace(task, num_retries=task.num_retries - 1)
            # return the item back to the queue
            await envelope.replace(task)
            return False
