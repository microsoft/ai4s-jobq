# Copyright (c) Microsoft Corporation.
# Licensed under the MIT License.
import typing as ty
from contextlib import asynccontextmanager
from datetime import timedelta
from types import TracebackType

from ai4s.jobq.entities import Response, Task


class Envelope(ty.Protocol):
    @property
    def task(self) -> Task: ...

    @property
    def id(self) -> str: ...

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
        exc_type: ty.Optional[ty.Type[BaseException]],
        exc: ty.Optional[BaseException],
        tb: ty.Optional[TracebackType],
    ) -> None: ...

    async def push(self, task: Task) -> str: ...

    def receive_message(
        self, visibility_timeout: timedelta, with_heartbeat: bool = False, **kwargs
    ) -> "ty.AsyncContextManager[Envelope]": ...

    async def create(self, exist_ok: bool = True) -> None: ...

    async def clear(self) -> None: ...

    async def __len__(self) -> int: ...

    def generate_sas(self, ttl: timedelta) -> str: ...

    async def peek(self, n: int = 1, as_json: bool = False) -> ty.Any: ...

    async def get_result(
        self, session_id: str, timeout: ty.Optional[timedelta] = None
    ) -> Response: ...

    @asynccontextmanager
    async def get_worker_interface(self, **kwargs) -> ty.AsyncGenerator[JobQBackendWorker, None]:
        yield self
