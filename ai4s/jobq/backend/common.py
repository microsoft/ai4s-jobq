# Copyright (c) Microsoft Corporation.
# Licensed under the MIT License.
import asyncio
import typing as ty
from contextlib import asynccontextmanager
from datetime import timedelta
from types import TracebackType

from ai4s.jobq.entities import Response, Task


class Envelope(ty.Protocol):
    """Represents a message received from the queue.

    .. note:: **Backend-specific behavior of ``replace()``**

       - **Storage Queue**: atomically updates the message content in-place
         (uses ``update_message``).
       - **Service Bus**: no-op—Service Bus has no in-place update API.
         The message will be re-delivered with its original content after
         the lock expires.  Users who need retry budgets on Service Bus
         should track retries in their own application code.
    """

    @property
    def task(self) -> Task: ...

    @property
    def id(self) -> str: ...

    @property
    def lock_lost_event(self) -> asyncio.Event: ...

    async def delete(self, success: bool, error: str | None = None) -> None: ...

    async def requeue(self) -> None: ...

    async def replace(self, task: Task) -> None: ...

    async def reply(self, response: Response) -> None: ...

    async def cancel_heartbeat(self) -> None: ...


class JobQBackendWorker(ty.Protocol):
    def receive_message(
        self, visibility_timeout: timedelta, with_heartbeat: bool = False, **kwargs
    ) -> ty.AsyncContextManager[Envelope]: ...

    async def push(self, task: Task) -> str: ...


class JobQBackend(ty.Protocol):
    @property
    def name(self) -> str: ...

    async def __aenter__(self) -> "JobQBackend": ...

    async def __aexit__(
        self,
        exc_type: type[BaseException] | None,
        exc: BaseException | None,
        tb: TracebackType | None,
    ) -> None: ...

    async def push(self, task: Task) -> str: ...

    def receive_message(
        self, visibility_timeout: timedelta, with_heartbeat: bool = False, **kwargs
    ) -> "ty.AsyncContextManager[Envelope]": ...

    async def create(self, exist_ok: bool = True) -> None: ...

    async def clear(self) -> None: ...

    async def __len__(self) -> int: ...

    async def peek(self, n: int = 1, as_json: bool = False) -> ty.Any: ...

    async def get_result(self, session_id: str, timeout: timedelta | None = None) -> Response: ...

    @asynccontextmanager
    async def get_worker_interface(self, **kwargs) -> ty.AsyncGenerator[JobQBackendWorker, None]:
        yield self
