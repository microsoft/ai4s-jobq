# Copyright (c) Microsoft Corporation.
# Licensed under the MIT License.
import asyncio
import logging
import typing as ty
from contextlib import AsyncExitStack, asynccontextmanager, suppress
from dataclasses import replace as replace_in_dataclass
from datetime import datetime, timedelta, timezone
from types import TracebackType

import azure.core.exceptions
from azure.core.credentials_async import AsyncTokenCredential
from azure.storage.queue import (
    QueueMessage,
    QueueSasPermissions,
    generate_queue_sas,
)
from azure.storage.queue.aio import QueueClient

from ai4s.jobq.entities import EmptyQueue, Response, Task

from .common import Envelope, JobQBackend

LOG = logging.getLogger(__name__)


class StorageQueueEnvelope(Envelope):
    def __init__(
        self,
        message: QueueMessage,
        task: Task,
        backend: "StorageQueueBackend",
        cancel_heartbeat_event: asyncio.Event,
        heartbeat_cancelled_event: asyncio.Event,
    ):
        self.message = message
        self.backend = backend
        self._task = task
        self.cancel_heartbeat_event = cancel_heartbeat_event
        self.heartbeat_cancelled_event = heartbeat_cancelled_event

    @property
    def id(self) -> str:
        return self.message.id

    @property
    def task(self) -> Task:
        return self._task

    async def delete(self, success: bool, error: str | None = None) -> None:
        if not success:
            await self.backend.add_to_dead_letter_queue(self.task, error)
        await self.backend.delete(self.message)

    async def cancel_heartbeat(self) -> None:
        LOG.debug("Cancelling heartbeat for %s", self.message.id)
        self.cancel_heartbeat_event.set()
        await self.heartbeat_cancelled_event.wait()

    async def requeue(self) -> None:
        LOG.debug("Requeueing %s", self.message.id)
        assert self.backend.queue_client is not None
        await self.backend.queue_client.update_message(
            self.message,
            content=self.task.serialize(),
            visibility_timeout=0,  # return back into the queue
        )

    async def replace(self, task: Task) -> None:
        assert self.backend.queue_client is not None
        self._task = task
        await self.backend.queue_client.update_message(
            self.message,
            content=task.serialize(),
            visibility_timeout=0,  # return back into the queue
        )

    async def reply(self, response: Response) -> None:
        raise NotImplementedError("Reply is not implemented for StorageQueueBackend")


class StorageQueueBackend(JobQBackend):
    def __init__(
        self,
        queue_name: str,
        *,
        storage_account: ty.Optional[str] = None,
        connection_string: ty.Optional[str] = None,
        credential: ty.Optional[ty.Union[str, AsyncTokenCredential]] = None,
    ):
        self.connection_string = connection_string
        self.storage_account = storage_account
        self.queue_name = queue_name
        self.queue_client: ty.Optional[QueueClient] = None
        self.dead_letter_queue_client: ty.Optional[QueueClient] = None
        self.credential = credential

        if self.queue_name == "my-unique-queue":
            raise ValueError("Don't be lazy and change the queue name to something unique.")

    @property
    def dead_letter_queue_name(self) -> str:
        return f"{self.queue_name}-failed"

    async def __aenter__(self) -> "StorageQueueBackend":
        if self.connection_string:
            self.queue_client = QueueClient.from_connection_string(
                self.connection_string, self.queue_name
            )
            self.dead_letter_queue_client = QueueClient.from_connection_string(
                self.connection_string, self.dead_letter_queue_name
            )
        elif self.credential:
            self.queue_client = QueueClient(
                account_url=f"https://{self.storage_account}.queue.core.windows.net",
                queue_name=self.queue_name,
                credential=self.credential,
            )
            self.dead_letter_queue_client = QueueClient(
                account_url=f"https://{self.storage_account}.queue.core.windows.net",
                queue_name=self.dead_letter_queue_name,
                credential=self.credential,
            )
        else:
            raise ValueError("Either connection_string or credential must be provided.")
        return self

    async def __aexit__(
        self,
        exc_type: ty.Optional[ty.Type[BaseException]],
        exc: ty.Optional[BaseException],
        tb: ty.Optional[TracebackType],
    ) -> None:
        assert self.queue_client is not None
        await self.queue_client.__aexit__(exc_type, exc, tb)  # type: ignore
        if self.dead_letter_queue_client is not None:
            await self.dead_letter_queue_client.__aexit__(exc_type, exc, tb)

    async def push(self, task: Task) -> str:
        assert self.queue_client is not None
        if task.reply_requested:
            raise NotImplementedError("Reply is not supported for StorageQueueBackend.")
        msg = await self.queue_client.send_message(task.serialize(), time_to_live=-1)
        return msg.id

    @asynccontextmanager
    async def receive_message(
        self, visibility_timeout: timedelta, with_heartbeat: bool = False, **kwargs
    ) -> ty.AsyncGenerator[StorageQueueEnvelope, None]:
        assert self.queue_client is not None
        envelope = await self.queue_client.receive_message(
            visibility_timeout=int(visibility_timeout.total_seconds())
        )
        if envelope is None:
            raise EmptyQueue(f"The queue {self.name} has no more tasks.")
        cancel_heartbeat_event = asyncio.Event()
        heartbeat_cancelled_event = asyncio.Event()
        async with AsyncExitStack() as stack:
            assert envelope is not None
            if with_heartbeat:
                heartbeat_interval = visibility_timeout.total_seconds() / 2

                assert (
                    heartbeat_interval > 0
                ), "Visibility timeout must be at least 2 seconds for heartbeat to work."

                await stack.enter_async_context(
                    self._heartbeat_worker(
                        envelope,
                        interval=heartbeat_interval,
                        visibility_timeout=visibility_timeout,
                        cancel_heartbeat_event=cancel_heartbeat_event,
                        heartbeat_cancelled_event=heartbeat_cancelled_event,
                    )
                )
            else:
                heartbeat_cancelled_event.set()

            try:
                task = Task.deserialize(envelope["content"])
            except Exception:
                LOG.error(
                    "Stopping processing due to deserialization error to prevent potential data loss.",
                    exc_info=True,
                )
                raise
            else:
                yield StorageQueueEnvelope(
                    envelope, task, self, cancel_heartbeat_event, heartbeat_cancelled_event
                )

    async def add_to_dead_letter_queue(self, task: Task, error: str | None = None) -> None:
        if self.dead_letter_queue_client is None:
            return
        task = replace_in_dataclass(task, error=error)
        LOG.debug("Adding task %r to dead letter queue %r", task.id, self.dead_letter_queue_name)
        await self.dead_letter_queue_client.send_message(task.serialize(), time_to_live=-1)

    async def delete(self, message: QueueMessage) -> None:
        assert self.queue_client is not None
        await self.queue_client.delete_message(message)

    async def create(self, exist_ok: bool = True) -> None:
        assert self.queue_client is not None
        try:
            await self.queue_client.create_queue()
        except azure.core.exceptions.ResourceExistsError:
            if not exist_ok:
                raise
        except azure.core.exceptions.HttpResponseError as e:
            if "invalid characters" in e.message:
                raise ValueError(
                    f"Invalid queue name '{self.queue_name}'. Only lowercase alphanumeric characters and hyphens are allowed."
                ) from e

        if self.dead_letter_queue_client is not None:
            try:
                await self.dead_letter_queue_client.create_queue()
            except azure.core.exceptions.ResourceExistsError:
                if not exist_ok:
                    raise

    async def clear(self) -> None:
        assert self.queue_client is not None
        await self.queue_client.clear_messages()

    async def __len__(self) -> int:
        assert self.queue_client is not None
        props = await self.queue_client.get_queue_properties()
        assert props.approximate_message_count is not None
        return props.approximate_message_count

    @property
    def name(self) -> str:
        assert self.queue_client is not None
        account_name = self.queue_client.account_name
        queue_name = self.queue_client.queue_name
        return f"{account_name}/{queue_name}"

    @asynccontextmanager
    async def _heartbeat_worker(
        self,
        message: QueueMessage,
        *,
        interval: float,
        visibility_timeout: timedelta = timedelta(hours=1),
        cancel_heartbeat_event: asyncio.Event,
        heartbeat_cancelled_event: asyncio.Event,
    ) -> ty.AsyncGenerator[None, None]:
        """Keeps a queue entry reserved by sending heartbeats.

        Args:
            message: The message to keep reserved.
            queue: The queue to send heartbeats to.
            interval: The interval at which to send heartbeats, in seconds.
            visibility_timeout: The message will stop being reserved if there is no heartbeat for this number of seconds.
        """

        async def _heartbeat() -> None:
            try:
                while not cancel_heartbeat_event.is_set():
                    pop_receipt = message.pop_receipt
                    try:
                        assert self.queue_client is not None

                        LOG.debug(
                            "Sending Heartbeat update for %s, %s",
                            message.id,
                            pop_receipt,
                        )
                        pop_receipt = (
                            await self.queue_client.update_message(
                                message,
                                pop_receipt=pop_receipt,
                                visibility_timeout=int(visibility_timeout.total_seconds()),
                                timeout=round(interval),
                            )
                        ).pop_receipt
                        message.pop_receipt = pop_receipt
                        LOG.debug(
                            "Received Heartbeat pop receipt for %s: %s",
                            message.id,
                            pop_receipt,
                        )
                    except Exception as e:
                        LOG.exception(
                            f"Failed to send heartbeat for message {message.id}: ", exc_info=e
                        )
                    with suppress(asyncio.TimeoutError):
                        await asyncio.wait_for(cancel_heartbeat_event.wait(), interval)
            finally:
                # inform anyone waiting that we are done with the heartbeat.
                # Note that we cannot use task.cancel here, because it might interrupt the update_message call.
                # This in turn would mean that we have a stale pop_receipt; we would not be able to delete the message.
                heartbeat_cancelled_event.set()

        task = asyncio.create_task(_heartbeat(), name="heartbeat")

        yield

        cancel_heartbeat_event.set()
        await heartbeat_cancelled_event.wait()

        try:
            await task
        except asyncio.CancelledError:
            pass
        LOG.debug("Done with heartbeat of %s", message.id)

    def generate_sas(self, ttl: timedelta) -> str:
        assert self.credential is not None, "Credential is required to generate SAS token."
        assert isinstance(
            self.credential, str
        ), "Credential needs to be of type str to generate SAS token."
        assert self.queue_client is not None
        assert (
            self.queue_client.account_name is not None
        ), "Account name is required to generate SAS token."
        return generate_queue_sas(
            account_name=self.queue_client.account_name,
            account_key=self.credential,
            queue_name=self.queue_client.queue_name,
            permission=QueueSasPermissions(read=True, process=True, update=True),
            start=datetime.now(timezone.utc),
            expiry=datetime.now(timezone.utc) + ttl,
        )

    async def peek(self, n: int = 1, as_json=False) -> ty.List[QueueMessage]:
        assert self.queue_client is not None
        return await self.queue_client.peek_messages(n)

    async def get_result(self, session_id: str, timeout: ty.Optional[timedelta] = None) -> Response:
        raise NotImplementedError("get_result is not implemented for StorageQueueBackend")
