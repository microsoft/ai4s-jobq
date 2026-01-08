# Copyright (c) Microsoft Corporation.
# Licensed under the MIT License.
import asyncio
import logging
import os
import textwrap
import time
import uuid
from contextlib import asynccontextmanager
from dataclasses import replace
from datetime import timedelta
from functools import partial
from inspect import signature
from typing import (
    Any,
    AsyncGenerator,
    Awaitable,
    Callable,
    Dict,
    Generator,
    Optional,
    Type,
    TypeVar,
    Union,
)

import azure.core.exceptions
from azure.core.credentials_async import AsyncTokenCredential

from ai4s.jobq.entities import Response, Task, WorkerCanceled

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
        self._result: Optional[Response] = None

    def __await__(self) -> Generator[Any, None, Response]:
        async def closure() -> Response:
            if self._result is not None:
                raise RuntimeError("This future has already been awaited.")
            self._result = await self.jobq.get_result(self.job_id)
            return self._result

        return closure().__await__()

    def result(self, timeout: Optional[timedelta] = None) -> Response:
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
        credential: Optional[Union[str, AsyncTokenCredential]] = None,
    ):
        self._client = backend
        self._credential = credential

    @classmethod
    @asynccontextmanager
    async def from_environment(
        cls: Type[T],
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
        if "JOBQ_STORAGE".startswith("sb://"):
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
        cls: Type[T],
        name: str,
        *,
        fqns: Optional[str] = None,
        credential: Optional[Any] = None,
        exist_ok: bool = True,
    ) -> AsyncGenerator[T, None]:
        """Creates a new queue from a Service Bus."""
        from .backend.servicebus import ServiceBusJobqBackend

        assert fqns is not None
        async with ServiceBusJobqBackend(
            queue_name=name,
            fqns=fqns,
            credential=credential,
            exist_ok=exist_ok,
        ) as backend:
            str_credential = credential if isinstance(credential, str) else None
            yield cls(backend, credential=str_credential)

    @classmethod
    @asynccontextmanager
    async def from_storage_queue(
        cls: Type[T],
        name: str,
        *,
        storage_account: str,
        credential: Optional[Union[str, AsyncTokenCredential]],
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

    async def sas_token(self, ttl: Optional[timedelta]) -> str:
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
        cls: Type[T],
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

    async def get_result(self, session: str, timeout: Optional[timedelta] = None) -> Response:
        """Retrieves the result of a task from the queue."""
        return await self._client.get_result(session, timeout)

    async def push(
        self,
        kwargs: Union[Dict[str, Any], str],
        *,
        num_retries: int = 5,
        reply_requested: bool = False,
        id: Optional[str] = None,
        worker_interface: Optional[JobQBackendWorker] = None,
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

        task = Task(
            id=id or str(uuid.uuid4()),
            kwargs=kwargs,
            num_retries=num_retries,
            reply_requested=reply_requested,
        )
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
        command_callback: Union[
            Callable[..., Awaitable[CallbackReturnType]],
            Callable[..., CallbackReturnType],
        ],
        *,
        visibility_timeout: timedelta = timedelta(minutes=10),
        with_heartbeat: bool = False,
        worker_id: Optional[str] = None,
        worker_interface: Optional[JobQBackendWorker] = None,
    ) -> bool:
        """Gets one task from the queue (first pushed, first pulled), verifies the signature, and executes the command.

        If the signature is invalid, the task is deleted from the queue.
        If the command fails, the task is re-queued with one less retry.
        If the command succeeds, the task is deleted from the queue.

        Args:
            visibility_timeout: If we send no heartbeat for this many seconds, the task is re-queued.

        Raises:
            EmptyQueue: If the queue is empty.

        Returns:
            True if the task was executed successfully, False otherwise.
        """
        # Export this environment variable such that inside the jobs we can know which queue we're working on.
        os.environ["JOBQ_QUEUE_NAME"] = self.full_name
        worker_interface = worker_interface if worker_interface is not None else self._client
        async with worker_interface.receive_message(
            visibility_timeout=visibility_timeout,
            with_heartbeat=with_heartbeat,
        ) as envelope:
            task = envelope.task
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
            ret: Optional[CallbackReturnType] = None
            try:
                if signature(command_callback).parameters.get("_job_id") is not None:
                    kwargs["_job_id"] = task.id
                if signature(command_callback).parameters.get("_worker_id") is not None:
                    kwargs["_worker_id"] = worker_id
                if _is_async_callable(command_callback):
                    ret = await command_callback(**kwargs)  # type: ignore
                else:
                    LOG.warning(
                        "Callback does not seem to be async. This is problematic if you're using heartbeats and/or multiple workers."
                    )
                    ret = command_callback(**kwargs)  # type: ignore
                execution_was_succesful = True
            except WorkerCanceled:
                await envelope.cancel_heartbeat()
                await envelope.requeue()
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
                await envelope.cancel_heartbeat()
            else:
                await envelope.cancel_heartbeat()

            duration = time.time() - start_time
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
                    LOG.warning("Failed to delete message: %s", e)
                return True
            else:
                # get our caching log handler
                log_handler = next(
                    (h for h in logging.getLogger().handlers if hasattr(h, "get_log_cache")), None
                )
                log = None
                if log_handler:
                    log = "\n".join(log_handler.get_log_cache(task.id))  # type: ignore
                    log = log[-100 * 256 :]  # truncate to equivalent of 100 lines of 256 chars

                LOG.exception(
                    f"Failure for task {task.id}.",
                    exc_info=exc,
                    extra={
                        "duration_s": duration,
                        "task_id": task.id,
                        "event": "task_failure",
                        "queue": self.full_name,
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
                        "queue": self.full_name,
                    },
                )
                task = replace(task, num_retries=task.num_retries - 1)
                # return the item back to the queue
                await envelope.replace(task)
                return False
