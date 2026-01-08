# Copyright (c) Microsoft Corporation.
# License under the MIT License.
import json
import logging
import os
import time
import typing as ty
from contextlib import asynccontextmanager
from datetime import timedelta
from types import TracebackType

from azure.core.credentials import AccessToken
from azure.core.credentials_async import AsyncTokenCredential
from azure.core.exceptions import ResourceExistsError
from azure.servicebus import (
    ServiceBusMessage,
    ServiceBusReceivedMessage,
    ServiceBusReceiveMode,
)
from azure.servicebus.aio import (
    AutoLockRenewer,
    ServiceBusClient,
    ServiceBusReceiver,
    ServiceBusSender,
)
from azure.servicebus.aio.management import ServiceBusAdministrationClient

from ai4s.jobq.entities import EmptyQueue, Response, Task

from .common import Envelope, JobQBackend

LOG = logging.getLogger(__name__)


class CachedTokenCredential:
    def __init__(self, credential: AsyncTokenCredential):
        self.credential = credential
        self.token: ty.Optional[AccessToken] = None

    async def __aenter__(self) -> "CachedTokenCredential":
        await self.credential.__aenter__()
        return self

    async def __aexit__(self, *args):
        await self.credential.__aexit__(*args)

    async def get_token(self, *scopes: str) -> AccessToken:
        if self.token is None or time.time() > self.token.expires_on - 60:
            token = await self.credential.get_token(*scopes)
            self.token = token

        return self.token


class ServiceBusEnvelope(Envelope):
    def __init__(
        self,
        message: ServiceBusReceivedMessage,
        task: Task,
        receiver: ServiceBusReceiver,
        sender: ServiceBusSender | None,
    ):
        self.message = message
        self.receiver = receiver
        self.sender = sender
        self._task = task
        self.done = False

    @property
    def id(self) -> str:
        assert self.message.message_id is not None
        return self.message.message_id

    @property
    def task(self) -> Task:
        return self._task

    async def cancel_heartbeat(self) -> None:
        # service bus manages heartbeats for us, so we can safely do nothing here.
        pass

    async def requeue(self) -> None:
        LOG.debug(f"Requeueing message {self.id}")
        await self.receiver.abandon_message(self.message)

    async def reply(self, response: Response) -> None:
        raise NotImplementedError("ServiceBus Backend does not support replies yet.")

    async def delete(self, success: bool, error: str | None = None) -> None:
        if success:
            await self.receiver.complete_message(self.message)
        else:
            # jobq may have configured a retry number threshold on the message which is lower
            # than the queue's max delivery count, so we dead letter the message here explicitly.
            await self.receiver.dead_letter_message(
                self.message, reason=error or "Failed", error_description=error or ""
            )
        self.done = True

    async def abandon(self) -> None:
        await self.receiver.abandon_message(self.message)
        self.done = True

    async def replace(self, task: Task) -> None:
        msg = ServiceBusMessage(
            body=task.serialize(), reply_to_session_id=self.message.reply_to_session_id
        )
        assert self.sender is not None, "Sender is not initialized."
        await self.sender.send_messages(msg)


class ServiceBusJobqBackend(JobQBackend):
    def __init__(
        self,
        queue_name: str,
        *,
        fqns: ty.Optional[str] = None,
        credential: ty.Optional[ty.Any] = None,
        exist_ok: bool = True,
    ):
        self.fqns = fqns
        self.queue_name = queue_name
        self.reply_queue_name = queue_name + "-replies"
        self.client: ty.Optional[ServiceBusClient] = None
        self.credential = credential
        self._exist_ok = exist_ok

    async def __aenter__(self) -> "ServiceBusJobqBackend":
        if self.credential is not None:
            assert self.fqns is not None
            credential = self.credential
            if isinstance(self.credential, AsyncTokenCredential):
                credential = CachedTokenCredential(self.credential)
            self.client = ServiceBusClient(
                fully_qualified_namespace=self.fqns,
                credential=credential,  # type: ignore
                logging_enable=False,
            )
        else:
            raise RuntimeError("No credential provided.")

        await self.create(exist_ok=self._exist_ok)
        self.client = await self.client.__aenter__()  # type: ignore
        return self

    async def __aexit__(
        self,
        exc_type: ty.Optional[ty.Type[BaseException]],
        exc: ty.Optional[BaseException],
        tb: ty.Optional[TracebackType],
    ) -> None:
        assert self.client is not None
        await self.client.__aexit__(exc_type, exc, tb)  # type: ignore

    async def push(self, task: Task) -> str:
        raise RuntimeError("Use a ServiceBusJobqBackendWorker to push messages to service bus")

    async def get_result(self, session_id: str, timeout: ty.Optional[timedelta] = None) -> Response:
        raise NotImplementedError("ServiceBus backend does not support get_result yet.")

    @asynccontextmanager  # type: ignore
    async def receive_message(
        self, visibility_timeout: timedelta, with_heartbeat: bool = False, **kwargs
    ) -> ty.AsyncGenerator[ServiceBusEnvelope, None]:
        raise RuntimeError("Use ServiceBusJobqBackendWorker to receive messages.")

    async def create(self, exist_ok: bool = True) -> None:
        async with self.get_admin_client() as admin_client:
            try:
                await admin_client.create_queue(queue_name=self.queue_name, requires_session=False)
                LOG.info(f"Created queue {self.queue_name}")
            except ResourceExistsError:
                if not exist_ok:
                    raise
            try:
                await admin_client.create_queue(
                    queue_name=self.reply_queue_name, requires_session=True
                )
                LOG.info(f"Created queue {self.reply_queue_name}")
            except ResourceExistsError:
                if not exist_ok:
                    raise

    async def clear(self) -> None:
        """
        Drain the queue, notifying potential consumers that the queue is being cleared.

        Note that the reply queue is *not* cleared here, it's up to the user to do this for now.
        """
        assert self.client is not None

        n = 0
        max_wait_time = int(os.environ.get("JOBQ_SERVICEBUS_MAX_WAIT_TIME", 2))
        batch_size = int(os.environ.get("JOBQ_SERVICEBUS_BATCH_SIZE", 250))
        async with self.client.get_queue_receiver(
            max_message_count=batch_size,
            queue_name=self.queue_name,
            receive_mode=ServiceBusReceiveMode.RECEIVE_AND_DELETE,
            prefetch_count=0,
            max_wait_time=max_wait_time,
        ) as receiver:
            while True:
                msgs = await receiver.receive_messages(
                    max_message_count=batch_size, max_wait_time=max_wait_time
                )
                n += len(msgs)
                if not msgs:
                    break
        LOG.info(f"Cleared {n} messages from queue {self.queue_name}.")

    @asynccontextmanager
    async def get_admin_client(self) -> ty.AsyncGenerator[ServiceBusAdministrationClient, None]:
        if self.credential is not None:
            assert self.fqns is not None, "Fully qualified namespace for service bus is required."
            async with ServiceBusAdministrationClient(
                fully_qualified_namespace=self.fqns,
                credential=self.credential,  # type: ignore
            ) as aclt:
                yield aclt
        else:
            raise RuntimeError("No credential provided.")

    async def __len__(self) -> int:
        async with self.get_admin_client() as aclt:
            queue_runtime_info = await aclt.get_queue_runtime_properties(queue_name=self.queue_name)
            ret = queue_runtime_info.active_message_count
            assert isinstance(ret, int)
            return ret

    @property
    def name(self) -> str:
        return f"{self.fqns}/{self.queue_name}"

    def generate_sas(self, ttl: timedelta) -> str:
        raise NotImplementedError("ServiceBus does not yet support SAS tokens.")

    async def peek(self, n: int = 1, as_json=False) -> ty.List[ServiceBusReceivedMessage]:
        assert self.client is not None
        messages: ty.List[ServiceBusReceivedMessage] = []
        MAX_PEEK_COUNT = 250
        async with self.client.get_queue_receiver(self.queue_name) as receiver:
            while True:
                page = await receiver.peek_messages(
                    max_message_count=min(n, MAX_PEEK_COUNT) if n > 0 else MAX_PEEK_COUNT,
                    sequence_number=messages[-1].sequence_number + 1
                    if messages and messages[-1].sequence_number is not None
                    else 0,
                )
                if not page:
                    break
                messages.extend(page)
                if n > 0 and len(messages) >= n:
                    break
            if not messages:
                raise EmptyQueue(f"The queue {self.name} has no more tasks.")
            messages = messages[:n] if n > 0 else messages
            if as_json:
                messages = [json.loads(list(m.body)[-1].decode()) for m in messages]
            return messages

    @asynccontextmanager
    async def get_worker_interface(
        self,
        receiver_kwargs=None,
        sender_kwargs=None,
        no_receiver: bool = False,
        no_sender: bool = False,
        **kwargs,
    ) -> ty.AsyncGenerator["ServiceBusJobqBackendWorker", None]:  # type: ignore
        interface = ServiceBusJobqBackendWorker(
            self, receiver_kwargs, sender_kwargs, no_receiver, no_sender
        )
        await interface.__aenter__()
        try:
            yield interface
        finally:
            await interface.__aexit__(None, None, None)


class ServiceBusJobqBackendWorker:
    def __init__(
        self,
        backend: ServiceBusJobqBackend,
        receiver_kwargs: dict | None,
        sender_kwargs: dict | None,
        no_receiver: bool = False,
        no_sender: bool = False,
    ):
        self.backend = backend
        assert backend.client is not None, "Backend client is not initialized."

        sender_kwargs = sender_kwargs or dict()
        receiver_kwargs = receiver_kwargs or dict()

        max_wait_time = int(os.environ.get("JOBQ_SERVICEBUS_MAX_WAIT_TIME", 5))
        prefetch_count = int(os.environ.get("JOBQ_SERVICEBUS_PREFETCH_COUNT", 0))
        receiver_kwargs.setdefault("receive_mode", ServiceBusReceiveMode.PEEK_LOCK)
        receiver_kwargs.setdefault("max_wait_time", max_wait_time)
        receiver_kwargs.setdefault("prefetch_count", prefetch_count)
        if no_receiver:
            self.receiver = None
        else:
            self.receiver = backend.client.get_queue_receiver(backend.queue_name, **receiver_kwargs)
        if no_sender:
            self.sender = None
        else:
            self.sender = backend.client.get_queue_sender(backend.queue_name, **sender_kwargs)
        self.lock_renewer: ty.Optional[AutoLockRenewer] = None

    async def __aenter__(self) -> "ServiceBusJobqBackendWorker":
        if self.receiver is not None:
            await self.receiver.__aenter__()
        if self.sender is not None:
            await self.sender.__aenter__()
        # if the message hasn't been processed in 3 weeks, the lock will not be renewed.
        self.lock_renewer = await AutoLockRenewer(60 * 60 * 24 * 21).__aenter__()
        return self

    async def __aexit__(self, exc_type, exc, tb) -> None:
        if self.receiver is not None:
            await self.receiver.__aexit__(exc_type, exc, tb)
        if self.sender is not None:
            await self.sender.__aexit__(exc_type, exc, tb)
        if self.lock_renewer is not None:
            await self.lock_renewer.__aexit__(exc_type, exc, tb)  # type: ignore

    @asynccontextmanager
    async def receive_message(
        self,
        visibility_timeout: timedelta,
        with_heartbeat: bool = False,
        **kwargs,
    ) -> ty.AsyncGenerator[ServiceBusEnvelope, None]:
        assert self.receiver is not None, "Receiver is not initialized."
        messages = await self.receiver.receive_messages(max_message_count=1, **kwargs)
        if not messages:
            raise EmptyQueue(f"The queue {self.backend.name} has no more tasks.")
        if with_heartbeat:
            assert (
                self.lock_renewer is not None
            ), "Lock renewer is not initialized, forgot to use 'async with'?."
            self.lock_renewer.register(self.receiver, messages[0])
        content = [m for m in messages[0].body]
        assert len(content) == 1
        message = messages[0]
        # message.delivery_count  # TODO gets incremented when lock expires or abandon is called. Not ideal for us.
        try:
            task = Task.deserialize(content[0])
        except Exception:
            LOG.error(
                "Stopping processing due to deserialization error to prevent potential data loss.",
                exc_info=True,
            )
            raise
        else:
            yield ServiceBusEnvelope(message, task, self.receiver, self.sender)

    async def push(self, task: Task) -> str:
        msg = ServiceBusMessage(body=task.serialize())
        assert self.sender is not None, "Sender is not initialized."
        await self.sender.send_messages(msg)
        assert msg.message_id is not None
        return msg.message_id
